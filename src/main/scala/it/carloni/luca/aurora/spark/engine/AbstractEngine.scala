package it.carloni.luca.aurora.spark.engine

import java.io.{File, FileNotFoundException}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.aurora.spark.data.LoggingRecord
import it.carloni.luca.aurora.time.DateFormat
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

abstract class AbstractEngine(private final val applicationPropertiesFile: String) {

  private final val logger = Logger.getLogger(getClass)
  protected final val sparkSession: SparkSession = getOrCreateSparkSession
  protected final val jobProperties: PropertiesConfiguration = new PropertiesConfiguration()
  loadJobProperties(applicationPropertiesFile)

  // JDBC OPTIONS AND DATAFRAME READER
  protected final val jdbcUrl: String = jobProperties.getString("jdbc.url")
  protected final val jdbcUser: String = jobProperties.getString("jdbc.user")
  protected final val jdbcPassword: String = jobProperties.getString("jdbc.password")
  protected final val jdbcUseSSL: String = jobProperties.getString("jdbc.useSSL")
  private final val jdbcOptions: Map[String, String] = Map(

    "url" -> jdbcUrl,
    "driver" -> jobProperties.getString("jdbc.driver.className"),
    "user" -> jdbcUser,
    "password" -> jdbcPassword,
    "useSSL" -> jdbcUseSSL
  )

  private final val jdbcReader: DataFrameReader = sparkSession.read
    .format("jdbc")
    .options(jdbcOptions)

  // DATABASES
  protected final val pcAuroraDBName: String = jobProperties.getString("database.pc_aurora")
  protected final val lakeCedacriDBName: String = jobProperties.getString("database.lake_cedacri")

  // TABLES
  protected final val mappingSpecificationTBLName: String = jobProperties.getString("table.mapping_specification.name")
  protected final val dataLoadLogTBLName: String = jobProperties.getString("table.sourceload_log.name")
  protected final val lookupTBLName: String = jobProperties.getString("table.lookup.name")

  private def getOrCreateSparkSession: SparkSession = {

    logger.info("Trying to get or create SparkSession")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    logger.info(s"Successfully got or created SparkSession for application \'${sparkSession.sparkContext.appName}\'")
    logger.info(s"Spark application UI url: ${sparkSession.sparkContext.uiWebUrl.get}")
    sparkSession
  }

  private def loadJobProperties(propertiesFile: String): Unit = {

    val loadPropertiesTry: Try[Unit] = Try(jobProperties.load(new File(propertiesFile)))
    loadPropertiesTry match {

      case Failure(exception) =>

        logger.error("Exception occurred while loading properties file. Stack trace: ", exception)
        throw exception

      case Success(_) => logger.info("Successfully loaded properties file")
    }
  }

  val createLogRecord: (String, Option[String], Option[String], String, Option[String]) => LoggingRecord =
    (branchName: String, bancllNameOpt: Option[String], dtBusinessDateOpt: Option[String],
     impactedTable: String, exceptionMsgOpt: Option[String]) => {

    import java.sql.{Date, Timestamp}
    import java.time.{Instant, ZoneId, ZonedDateTime}

    val applicationId: String = sparkSession.sparkContext.applicationId
    val applicationName: String = sparkSession.sparkContext.appName
    val dtBusinessDateSQLDateOpt: Option[Date] = if (dtBusinessDateOpt.nonEmpty) {

      val dtBusinessDateString: String = dtBusinessDateOpt.get
      logger.info(s"Converting $dtBusinessDateString to java.sql.Date")
      val dtBusinessDateLocalDate: LocalDate = LocalDate.parse(dtBusinessDateString,
        DateTimeFormatter.ofPattern(DateFormat.DtBusinessDate.format))

      val dtBusinessDateSQLDateOpt: Option[Date] = Some(Date.valueOf(dtBusinessDateLocalDate))
      logger.info(s"Successfullt converted $dtBusinessDateString to java.sql.Date")
      dtBusinessDateSQLDateOpt

    } else None

    val applicationStartTime: Timestamp = Timestamp.from(Instant.ofEpochMilli(sparkSession.sparkContext.startTime))
    val applicationEndTime: Timestamp = Timestamp.from(ZonedDateTime.now(ZoneId.of("Europe/Rome")).toInstant)
    val applicationFinishCode: Int = if (exceptionMsgOpt.isEmpty) 0 else -1
    val applicationFinishStatus: String = if (exceptionMsgOpt.isEmpty) "SUCCESSED" else "FAILED"

    LoggingRecord(applicationId,
      applicationName,
      branchName,
      applicationStartTime,
      applicationEndTime,
      bancllNameOpt,
      dtBusinessDateSQLDateOpt,
      impactedTable,
      exceptionMsgOpt,
      applicationFinishCode,
      applicationFinishStatus)
  }

  protected def writeLogRecords(loggingRecords: Seq[LoggingRecord]): Unit = {

    import sparkSession.implicits._

    logger.info("Trying to turn Seq of logging records to spark.sql.DataFrame")

    val loggingRecordsDataset: DataFrame = loggingRecords.toDF()

    logger.info("Successfully turned Seq of logging records to spark.sql.DataFrame")
    writeToJDBC(loggingRecordsDataset, pcAuroraDBName, dataLoadLogTBLName, SaveMode.Append)
  }

  protected def readFromJDBC(databaseName: String, tableName: String): DataFrame = {

    logger.info(s"Starting to load table \'$databaseName\'.\'$tableName\'")

    val tryLoadJDBCDf: Try[DataFrame] = Try(jdbcReader
      .option("dbtable", s"$databaseName.$tableName")
      .load())

    tryLoadJDBCDf match {

      case Failure(exception) =>

        logger.error(s"Error while trying to read table \'$databaseName\'.\'$tableName\'")
        logger.error("Exception stack trace: ", exception)
        throw exception

      case Success(value) =>

        logger.info(s"Successfully loaded table \'$databaseName\'.\'$tableName\'")
        value
    }
  }

  protected def retrieveStructTypeFromXMLFile(xmlFilePath: String): StructType = {

    def resolveDataType(columnType: String): DataType = {

      columnType.toLowerCase match {

        case "string" => DataTypes.StringType
        case "int" => DataTypes.IntegerType
        case "date" => DataTypes.DateType
        case "timestamp" => DataTypes.TimestampType
      }
    }

    logger.info(s"XML file path: $xmlFilePath")
    val xmlSchemaFile: File = new File(xmlFilePath)
    if (xmlSchemaFile.exists()) {

      logger.info(s"XML file \'$xmlFilePath\' exists. So, trying to infer table schema from it")
      val mappingSpecificationXML: Elem = XML.loadFile(xmlSchemaFile)
      val columnSpecifications: Seq[(String, String, String)] = (mappingSpecificationXML \\ "tableSchema" \\ "columns" \\ "column")
        .map(columnTag => (columnTag.attribute("name").get.text,
          columnTag.attribute("type").get.text,
          columnTag.attribute("nullable").get.text))

      StructType(columnSpecifications.map(tuple3 => {

        val columnName: String = tuple3._1
        val columnType: DataType = resolveDataType(tuple3._2.toLowerCase)
        val nullable: Boolean = if (tuple3._3.toLowerCase == "true") true else false

        logger.info(s"Defining column with name \'$columnName\', type \'$columnType\', nullable \'$nullable\'")
        StructField(columnName, columnType, nullable)
      }))

    } else {

      val exceptionMsg: String = s"File \'$xmlFilePath\' does not exists (or cannot be found)"
      logger.error(exceptionMsg)
      throw new FileNotFoundException(s"File \'$xmlFilePath\' does not exists (or cannot be found)")
    }
  }

  protected def writeToJDBC(outputDataFrame: DataFrame, databaseName: String, tableName: String, saveMode: SaveMode): Unit = {

    logger.info(s"Starting to save dataframe into table \'$databaseName\'.\'$tableName\' using savemode \'$saveMode\'")
    logger.info(f"Dataframe schema: \n\n${outputDataFrame.schema.treeString}")

    val tryWriteDfToJDBC: Try[Unit] = Try(outputDataFrame.write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", s"$databaseName.$tableName")
      .mode(saveMode)
      .save())

    tryWriteDfToJDBC match {

      case Failure(exception) =>

        logger.error(s"Error while saving data to table \'$databaseName\'.\'$tableName\' using savemode \'$saveMode\'")
        logger.error("Exception stack trace: ", exception)
        throw exception

      case Success(_) =>

        logger.info(s"Successfully saved dataframe as table \'$databaseName\'.\'$tableName\' using savemode \'$saveMode\'")
    }
  }
}
