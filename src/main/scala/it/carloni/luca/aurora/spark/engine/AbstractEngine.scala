package it.carloni.luca.aurora.spark.engine

import java.io.{File, FileNotFoundException}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}

import it.carloni.luca.aurora.spark.data.LogRecord
import it.carloni.luca.aurora.utils.DateFormat
import it.carloni.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow, resolveDataType}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

abstract class AbstractEngine(private final val applicationPropertiesFile: String) {

  private final val logger = Logger.getLogger(getClass)
  protected final val sparkSession: SparkSession = getOrCreateSparkSession
  protected final val jobProperties: PropertiesConfiguration = new PropertiesConfiguration()
  loadJobProperties(applicationPropertiesFile)

  // JDBC OPTIONS AND DATAFRAME READER
  private final val jdbcOptions: Map[String, String] = Map(

    "url" -> jobProperties.getString("jdbc.url"),
    "driver" -> jobProperties.getString("jdbc.driver.className"),
    "user" -> jobProperties.getString("jdbc.user"),
    "password" -> jobProperties.getString("jdbc.password"),
    "useSSL" -> jobProperties.getString("jdbc.useSSL")
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

  protected val createLogRecord: (String, Option[String], Option[String], String, Option[String]) => LogRecord =
    (branchName, bancllNameOpt, dtBusinessDateOpt, impactedTable, exceptionMsgOpt) => {

      val applicationId: String = sparkSession.sparkContext.applicationId
      val applicationName: String = sparkSession.sparkContext.appName
      val dtBusinessDateSQLDateOpt: Option[Date] = if (dtBusinessDateOpt.nonEmpty) {

        val dtBusinessDateString: String = dtBusinessDateOpt.get
        logger.info(s"Converting $dtBusinessDateString to java.sql.Date")
        val dtBusinessDateLocalDate: LocalDate = LocalDate.parse(dtBusinessDateString, DateFormat.DT_RIFERIMENTO.getFormatter)

        val dtBusinessDateSQLDateOpt: Option[Date] = Some(Date.valueOf(dtBusinessDateLocalDate))
        logger.info(s"Successfullt converted $dtBusinessDateString to java.sql.Date")
        dtBusinessDateSQLDateOpt

      } else None

      val applicationStartTime: Timestamp = Timestamp.from(Instant.ofEpochMilli(sparkSession.sparkContext.startTime))
      val applicationEndTime: Timestamp = Timestamp.from(ZonedDateTime.now(ZoneId.of("Europe/Rome")).toInstant)
      val applicationFinishCode: Int = if (exceptionMsgOpt.isEmpty) 0 else -1
      val applicationFinishStatus: String = if (exceptionMsgOpt.isEmpty) "SUCCESSED" else "FAILED"

      LogRecord(applicationId,
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

  protected def readFromJDBC(databaseName: String, tableName: String): DataFrame = {

    logger.info(s"Starting to load table '$databaseName'.'$tableName'")

    Try {

      jdbcReader
        .option("dbtable", s"$databaseName.$tableName")
        .load()
    } match {

      case Failure(exception) =>

        logger.error(s"Error while trying to read table '$databaseName'.'$tableName'. Stack trace: ", exception)
        throw exception

      case Success(value) =>

        logger.info(s"Successfully loaded table '$databaseName'.'$tableName'")
        value
    }
  }

  protected def getMappingSpecificationDf: DataFrame = {

    getDfFromTSVFileAndXMLSchema(jobProperties.getString("table.mapping_specification.file.path"),
      jobProperties.getString("table.mapping_specification.file.sep"),
      jobProperties.getBoolean("table.mapping_specification.file.header"),
      jobProperties.getString("table.mapping_specification.xml.schema.path"))
  }

  protected def getLookUpDf: DataFrame = {

    getDfFromTSVFileAndXMLSchema(jobProperties.getString("table.lookup.file.path"),
      jobProperties.getString("table.lookup.file.sep"),
      jobProperties.getBoolean("table.lookup.file.header"),
      jobProperties.getString("table.lookup.xml.schema.path"))
  }

  protected def tryWriteToJDBCAndLog(op: (=> DataFrame),
                                     databaseName: String,
                                     tableName: String,
                                     saveMode: SaveMode,
                                     truncate: Boolean,
                                     logRecordFactory: (String, Option[String]) => LogRecord): Unit = {

    Try {

      writeToJDBC(op, databaseName, tableName, saveMode, truncate)

    } match {
      case Failure(exception) =>

        val details: String = s"'$databaseName'.'$tableName' with savemode '$saveMode'"
        logger.error(s"Caught exception while trying to save data into $details. Stack trace: ", exception)
        writeLogRecords(Seq(logRecordFactory(tableName, Some(exception.getMessage))))

      case Success(_) => writeLogRecords(Seq(logRecordFactory(tableName, None)))
    }
  }

  /* PRIVATE AREA */

  private def fromXMLToStructType(xmlFilePath: String): StructType = {

    val xmlSchemaFile: File = new File(xmlFilePath)
    if (xmlSchemaFile.exists()) {

      logger.info(s"XML file '$xmlFilePath' exists. So, trying to infer table schema from it")
      val mappingSpecificationXML: Elem = XML.loadFile(xmlSchemaFile)
      val columnSpecifications: Seq[(String, String, String)] = (mappingSpecificationXML \\ "tableSchema" \\ "columns" \\ "column")
        .map(columnTag => (columnTag.attribute("name").get.text,
          columnTag.attribute("type").get.text,
          columnTag.attribute("nullable").get.text))

      StructType(columnSpecifications.map(tuple3 => {

        val columnName: String = tuple3._1
        val columnType: DataType = resolveDataType(tuple3._2.toLowerCase)
        val nullable: Boolean = if (tuple3._3.toLowerCase == "true") true else false

        logger.info(s"Defining column with name '$columnName', type '$columnType', nullable '$nullable'")
        StructField(columnName, columnType, nullable)
      }))

    } else {

      val exceptionMsg: String = s"File '$xmlFilePath' does not exists (or cannot be found)"
      logger.error(exceptionMsg)
      throw new FileNotFoundException(exceptionMsg)
    }
  }

  private def getDfFromTSVFileAndXMLSchema(tsvFilePath: String, tsvSep: String, tsvHeader: Boolean, xMLSchemaFilePath: String): DataFrame = {

    logger.info(s"Attempting to load .tsv file at path '$tsvFilePath' (separator: '$tsvSep', file header presence: '$tsvHeader') as a spark.sql.DataFrame")

    val tsvFileDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", tsvFilePath)
      .option("sep", tsvSep)
      .option("header", tsvHeader)
      .schema(fromXMLToStructType(xMLSchemaFilePath))
      .load()
      .withColumn("ts_inizio_validita", lit(getJavaSQLTimestampFromNow))
      .withColumn("dt_inizio_validita", lit(getJavaSQLDateFromNow))

    logger.info(s"Successfully loaded .tsv file as a spark.sql.DataFrame")
    tsvFileDf
  }

  private def writeLogRecords(loggingRecords: Seq[LogRecord]): Unit = {

    import sparkSession.implicits._

    logger.info("Trying to turn Seq of logging records to spark.sql.DataFrame")

    val loggingRecordsDataset: DataFrame = loggingRecords.toDF()

    logger.info("Successfully turned Seq of logging records to spark.sql.DataFrame")
    writeToJDBC(loggingRecordsDataset, pcAuroraDBName, dataLoadLogTBLName, SaveMode.Append, truncate = false)
  }

  private def writeToJDBC(outputDataFrame: DataFrame, databaseName: String, tableName: String,
                          saveMode: SaveMode, truncate: Boolean): Unit = {

    val truncateOptionValue: String = if (truncate & (saveMode == SaveMode.Overwrite)) "true" else "false"
    val savingDetails: String = s"table: '$databaseName'.'$tableName', savemode: '$saveMode', truncate: '$truncateOptionValue'"
    logger.info(s"Starting to save dataframe into $savingDetails")
    logger.info(f"Dataframe schema:\n\n${outputDataFrame.schema.treeString}")

    outputDataFrame.write
      .format("jdbc")
      .options(jdbcOptions)
      .option("dbtable", s"$databaseName.$tableName")
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

  private def loadJobProperties(propertiesFile: String): Unit = {

    Try(jobProperties.load(new File(propertiesFile))) match {

      case Failure(exception) =>

        logger.error("Exception occurred while loading properties file. Stack trace: ", exception)
        throw exception

      case Success(_) => logger.info("Successfully loaded properties file")
    }
  }
}
