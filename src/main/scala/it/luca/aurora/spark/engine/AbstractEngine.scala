package it.luca.aurora.spark.engine

import java.sql.{Connection, DriverManager}

import it.luca.aurora.spark.data.LogRecord
import it.luca.aurora.utils.ColumnName
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

abstract class AbstractEngine(val jobPropertiesFile: String) {

  private final val logger = Logger.getLogger(getClass)
  protected final val sparkSession = getOrCreateSparkSession
  protected final val jobProperties = new PropertiesConfiguration(jobPropertiesFile)

  // JDBC options for Dataframe reader
  private final val jdbcOptions: Map[String, String] = Map(

    "url" -> jobProperties.getString("jdbc.url"),
    "driver" -> jobProperties.getString("jdbc.driver.className"),
    "user" -> jobProperties.getString("jdbc.user"),
    "password" -> jobProperties.getString("jdbc.password"),
    "useSSL" -> jobProperties.getString("jdbc.useSSL")
  )

  private final val technicalTimestampTypeColumns: Seq[String] =
    (ColumnName.TsInserimento :: ColumnName.TsInizioValidita :: ColumnName.TsFineValidita :: Nil)
    .map(_.name)

  // Databases
  protected final val pcAuroraDBName: String = jobProperties.getString("jdbc.database.pcAuroraAnalysis")
  protected final val lakeCedacriDBName: String = jobProperties.getString("jdbc.database.lakeCedacri")

  // Tables
  protected final val mappingSpecificationTBLName: String = jobProperties.getString("jdbc.table.mappingSpecification.actual")
  protected final val dataLoadLogTBLName: String = jobProperties.getString("jdbc.table.dataloadLog")
  protected final val lookupTBLName: String = jobProperties.getString("jdbc.table.lookup.actual")

  protected def getJDBCConnection: java.sql.Connection = {

    val jdbcURL: String = jobProperties.getString("jdbc.url")
    val jdbcUser: String = jobProperties.getString("jdbc.user")
    val jdbcPassword: String = jobProperties.getString("jdbc.password")
    val jdbcUseSSL: String = jobProperties.getString("jdbc.useSSL")

    Class.forName("com.mysql.jdbc.Driver")

    val jdbcUrlConnectionStr: String = s"$jdbcURL/?useSSL=$jdbcUseSSL"
    logger.info(s"Attempting to connect to JDBC url '$jdbcUrlConnectionStr' with credentials ('$jdbcUser', '$jdbcPassword')")

    val jdbcConnection: Connection = DriverManager.getConnection(jdbcUrlConnectionStr,
      jobProperties.getString("jdbc.user"),
      jobProperties.getString("jdbc.password"))

    logger.info(s"Successfully connected to JDBC url '$jdbcUrlConnectionStr' with credentials ('$jdbcUser', '$jdbcPassword')")
    jdbcConnection
  }

  protected def readFromJDBC(databaseName: String, tableName: String): DataFrame = {

    logger.info(s"Starting to load table '$databaseName'.'$tableName'")
    val jdbcDf: DataFrame = sparkSession.read
        .format("jdbc")
        .options(jdbcOptions)
        .option("dbtable", s"$databaseName.$tableName")
        .load()

    logger.info(s"Successfully loaded table '$databaseName'.'$tableName'")
    jdbcDf
  }

  protected def writeToJDBCAndLog[T](dbName: String,
                                     tableName: String,
                                     saveMode: SaveMode,
                                     truncateFlag: Boolean,
                                     logRecordFunction: (String, String, Option[String]) => LogRecord,
                                     dataframeFunction: T => DataFrame,
                                     dataframeFunctionArg: T): Unit = {

    import sparkSession.implicits._

    val exceptionMsgOpt: Option[String] = Try {

      val dfToWrite: DataFrame = dataframeFunction(dataframeFunctionArg)
      executeCreateTableIfDfContainsTimestamps(dbName, tableName, dfToWrite)
      writeToJDBC(dfToWrite, dbName, tableName, saveMode, truncateFlag)

    } match {
      case Failure(exception) =>

        val details: String = s"'$dbName.$tableName' with savemode '$saveMode'"
        logger.error(s"Caught exception while trying to save data into $details. Stack trace: ", exception)
        val firstNStackTraceStrings: String = exception.getStackTrace
          .toSeq
          .take(5)
          .map(_.toString)
          .mkString("\n")

        Some(s"${exception.toString}. Stack trace: \n" + firstNStackTraceStrings)

      case Success(_) => None
    }

    val logRecordDf: DataFrame = (logRecordFunction(dbName, tableName, exceptionMsgOpt) :: Nil).toDF
    writeToJDBC(logRecordDf, pcAuroraDBName, dataLoadLogTBLName, SaveMode.Append, truncate = false)
  }

  /* PRIVATE AREA */

  private final val executeCreateTableIfDfContainsTimestamps: (String, String, DataFrame) => Unit =
    (dbName, tableName, df) => {

    val nonTechnicalTimestampTypeColumns: Seq[String] = df.dtypes
      .filter(t => {

        val (columnName, columnType): (String, String) = t
        (columnType equalsIgnoreCase "timestamptype") && (!technicalTimestampTypeColumns.contains(columnName))
      })
      .map(t => t._1)

    /**
     * If a non-technical column has timestamp type,
     * we must first create the table via JDBC connection in order to set "datetime" type for such column.
     * This is due to the limited timestamp range on MySQL.
     *
     * Indeed, from MySQL docs:
     *
     * The TIMESTAMP data type has a range of '1970-01-01 00:00:01' UTC to '2038-01-09 03:14:07' UTC
     * The DATETIME data type has a range of '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
     */

    if (nonTechnicalTimestampTypeColumns.nonEmpty) {

      logger.info(s"Identified ${nonTechnicalTimestampTypeColumns.size} timestamp column(s) different from " +
        s"${technicalTimestampTypeColumns.map(x => s"'$x'").mkString(", ")}: " +
        s"${nonTechnicalTimestampTypeColumns.map(x => s"'$x'").mkString(", ")}")

      val jdbcConnection: java.sql.Connection = getJDBCConnection
      val existsCurrentTable: Boolean = jdbcConnection.getMetaData
        .getTables(dbName, null, tableName, null)
        .next()

      if (existsCurrentTable) {
        logger.info(s"Table '$dbName.$tableName' has some timestamp columns but it exists already. Thus, not creating it again")
      } else {

        logger.warn(s"Table '$dbName.$tableName' has some timestamp columns but it does not exist yet. Thus, defining and creating it now")
        val createTableStatement: java.sql.Statement = jdbcConnection.createStatement
        createTableStatement.execute(getCreateTableStatementFromDfSchema(df, dbName, tableName))
        logger.info(s"Successfully created table '$dbName.$tableName'")
      }
      jdbcConnection.close()
    }
  }

  private final val getCreateTableStatementFromDfSchema: (DataFrame, String, String) => String =
    (df, dbName, tableName) => {

      val fromSparkTypeToMySQLType: ((String, String)) => String = tuple2 => {
        val (columnName, columnType): (String, String) = tuple2
        columnType.toLowerCase match {
          case "stringtype" => s"TEXT"
          case "integertype" => "INT"
          case "doubletype" => "DOUBLE"
          case "longtype" => "BIGINT"
          case "datetype" => "DATE"
          case "timestamptype" => if (technicalTimestampTypeColumns.contains(columnName)) "TIMESTAMP" else "DATETIME"
        }
      }

      val createTableStateMent: String = s" CREATE TABLE IF NOT EXISTS $dbName.$tableName (\n" +
        df.dtypes.map(x => s"    ${x._1} ${fromSparkTypeToMySQLType(x)}").mkString(",\n") + " )\n"

      logger.info(s"Create table statement for table '$dbName'.'$tableName': \n\n $createTableStateMent")
      createTableStateMent
    }

  private def writeToJDBC(outputDataFrame: DataFrame, dbName: String, tableName: String,
                          saveMode: SaveMode, truncate: Boolean): Unit = {

    val truncateOptionValue: String = if (truncate & (saveMode == SaveMode.Overwrite)) "true" else "false"
    val savingDetails: String = s"table: '$dbName.$tableName', savemode: '$saveMode', truncate: '$truncateOptionValue'"
    logger.info(s"Starting to save dataframe into $savingDetails")
    logger.info(f"Dataframe schema:\n\n${outputDataFrame.schema.treeString}")

    outputDataFrame.write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", s"$dbName.$tableName")
      .option("truncate", truncateOptionValue)
      .mode(saveMode)
      .save()

    logger.info(s"Successfully saved data into $savingDetails")
  }

  private def getOrCreateSparkSession: SparkSession = {

    logger.info("Trying to get or create SparkSession")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val sparkContext: SparkContext = sparkSession.sparkContext
    logger.info(s"Successfully created SparkSession for application '${sparkContext.appName}'. " +
      s"Application Id: '${sparkContext.applicationId}', UI url: ${sparkContext.uiWebUrl.get}")
    sparkSession
  }
}
