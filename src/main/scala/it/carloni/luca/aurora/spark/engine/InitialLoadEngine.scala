package it.carloni.luca.aurora.spark.engine

import java.sql._
import java.time.{Instant, ZoneId, ZonedDateTime}

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.spark.data.LoggingRecord
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit

import scala.util.{Failure, Success, Try}

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createInitialLoadLogRecord = createLogRecord(Branch.InitialLoad.name, None, None, _: String, _: Option[String])

  def run(): Unit = {

    Class.forName("com.mysql.jdbc.Driver")

    // CREATE DATABASE, IF IT DOES NOT EXIST
    val jdbcUrlUseSSLConnectionString: String = s"$jdbcUrl/?useSSL=$jdbcUseSSL"
    logger.info(s"Attempting to connect to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")
    val jdbcConnection: Connection = DriverManager.getConnection(jdbcUrlUseSSLConnectionString, jdbcUser, jdbcPassword)
    logger.info(s"Successfully connected to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")
    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)

    // TABLE LOADING
    val stringToFunctionMap: Map[String, () => Unit] = Map(

      mappingSpecificationTBLName -> loadMappingSpecificationTable,
      lookupTBLName -> loadLookupTable
    )

    // FOR EACH TABLE TRY:
    // [a] TO EXECUTE THE RELATED LOADING PROCESS
    // [b] DEFINE THE RELATED LOG RECORD

    val initialLoadLogRecords: Seq[LoggingRecord] = (for ((tableName, function) <- stringToFunctionMap) yield {

      val functionExceptionMsgOpt: Option[String] = Try(function()) match {

        case Failure(exception) =>

          logger.error(s"Error while loading table \'$pcAuroraDBName\'.\'$tableName\'")
          logger.error("Exception stack trace: ", exception)
          Some(exception.getMessage)

        case Success(_) =>

          logger.info(s"Successfully loaded table \'$pcAuroraDBName\'.\'$tableName\'")
          None
      }

      createInitialLoadLogRecord(tableName, functionExceptionMsgOpt)
    }).toSeq

    writeLogRecords(initialLoadLogRecords)

    logger.info("Attempting to close JDBC connection")
    jdbcConnection.close()
    logger.info("Successfully closed JDBC connection")
  }

  private def createDatabaseIfNotExists(databaseToCreate: String, connection: Connection): Unit = {

    // RESULT SET CONTAINING DATABASE NAMES
    val resultSet: ResultSet = connection
      .getMetaData
      .getCatalogs

    // EXTRACT THOSE NAMES
    val existingDatabases: Seq[String] = Iterator.continually((resultSet.next(), resultSet))
      .takeWhile(_._1)
      .map(_._2.getString("TABLE_CAT"))
      .map(_.toLowerCase)
      .toSeq

    logger.info(s"Existing databases: ${existingDatabases
      .map(existingDatabase => s"\'$existingDatabase\'")
      .mkString(", ")}")

    val databaseToCreateLower: String = databaseToCreate.toLowerCase
    if (existingDatabases.contains(databaseToCreateLower))

      logger.info(s"Database \'$databaseToCreateLower\' already exists. So, not much to do ;)")

    else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseToCreateLower")
      logger.info(s"Successfully created database \'$databaseToCreateLower\'")
    }
  }

  private def loadMappingSpecificationTable(): Unit = {

    val tsvFilePath: String = jobProperties.getString("table.mapping_specification.file.path")
    val tsvFileSep: String = jobProperties.getString("table.mapping_specification.file.sep")
    val tsvFileHeader: Boolean = jobProperties.getBoolean("table.mapping_specification.file.header")
    val tsvFileXMLSchemaFilePath: String = jobProperties.getString("table.mapping_specification.xml.schema.path")

    logger.info(s"Mapping specification file: $tsvFilePath")
    logger.info(s"Separator to be used for file reading: $tsvFileSep")
    logger.info(s"Does the file has a header? $tsvFileHeader")

    logger.info(s"Attempting to load mapping specification file as a spark.sql.DataFrame")

    val nowAtRomeInstant: Instant = ZonedDateTime.now(ZoneId.of("Europe/Rome")).toInstant
    val mappingSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", tsvFilePath)
      .option("sep", tsvFileSep)
      .option("header", tsvFileHeader)
      .schema(retrieveStructTypeFromXMLFile(tsvFileXMLSchemaFilePath))
      .load()
      .withColumn("ts_inizio_validita", lit(Timestamp.from(nowAtRomeInstant)))
      .withColumn("dt_inizio_validita", lit(new Date(nowAtRomeInstant.toEpochMilli)))

    logger.info(s"Successfully loaded mapping specification file as a spark.sql.DataFrame")

    writeToJDBC(mappingSpecificationDf, pcAuroraDBName, mappingSpecificationTBLName, SaveMode.Overwrite)
  }

  private def loadLookupTable(): Unit = {

    val tsvFilePath: String = jobProperties.getString("table.lookup.file.path")
    val tsvFileSep: String = jobProperties.getString("table.lookup.file.sep")
    val tsvFileHeader: Boolean = jobProperties.getBoolean("table.lookup.file.header")
    val tsvFileXMLSchemaFilePath: String = jobProperties.getString("table.lookup.xml.schema.path")

    logger.info(s"Lookup specification file: $tsvFilePath")
    logger.info(s"Separator to be used for file reading: $tsvFileSep")
    logger.info(s"Does the file has a header? $tsvFileHeader")

    logger.info(s"Attempting to load lookup specification file as a spark.sql.DataFrame")

    val lookupSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", tsvFilePath)
      .option("sep", tsvFileSep)
      .option("header", tsvFileHeader)
      .schema(retrieveStructTypeFromXMLFile(tsvFileXMLSchemaFilePath))
      .load()

    logger.info(s"Successfully loaded lookup specification file as a spark.sql.DataFrame")
    writeToJDBC(lookupSpecificationDf, pcAuroraDBName, lookupTBLName, SaveMode.Overwrite)
  }
}

