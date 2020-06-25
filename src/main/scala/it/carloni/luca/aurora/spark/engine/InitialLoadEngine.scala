package it.carloni.luca.aurora.spark.engine

import java.sql._

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.spark.data.LoggingRecord
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(): Unit = {

    Class.forName("com.mysql.jdbc.Driver")

    val jdbcUrlUseSSLConnectionString: String = s"$jdbcUrl/?useSSL=$jdbcUseSSL"
    logger.info(s"Attempting to connect to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")
    val jdbcConnection: Connection = DriverManager.getConnection(jdbcUrlUseSSLConnectionString, jdbcUser, jdbcPassword)
    logger.info(s"Successfully connected to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")

    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)
    val mappingSpecificationLoadExceptionOpt: Option[String] = Try(loadMappingSpecificationTable()) match {

      case Failure(exception) =>

        logger.error(s"Error while trying to load table \'$pcAuroraDBName\'.\'$mappingSpecificationTBLName\'")
        logger.error("Exception stack trace: " , exception)
        Some(exception.getMessage)

      case Success(_) =>

        logger.info(s"Successfully loaded table \'$pcAuroraDBName\'.\'$mappingSpecificationTBLName\'")
        None
    }

    val lookupLoadExceptionOpt: Option[String] = Try(loadLookupTable()) match {

      case Failure(exception) =>

        logger.error(s"Error while trying to load table \'$pcAuroraDBName\'.\'$lookupTBLName\'")
        logger.error("Exception stack trace: " , exception)
        Some(exception.getMessage)

      case Success(_) =>

        logger.info(s"Successfully loaded table \'$pcAuroraDBName\'.\'$lookupTBLName\'")
        None
    }

    val loggingRecords: Seq[LoggingRecord] = Seq(
      createLoggingRecord(Branch.InitialLoad.name, None, None, mappingSpecificationTBLName, mappingSpecificationLoadExceptionOpt),
      createLoggingRecord(Branch.InitialLoad.name, None, None, lookupTBLName, lookupLoadExceptionOpt))

    insertLoggingRecords(loggingRecords)

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

    val databaseToCreatelower: String = databaseToCreate.toLowerCase
    if (existingDatabases.contains(databaseToCreatelower))

      logger.info(s"Database \'$databaseToCreatelower\' already exists. So, not much to do ;)")

    else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseToCreatelower")
      logger.info(s"Successfully created database \'$databaseToCreatelower\'")
    }
  }

  private def loadMappingSpecificationTable(): Unit = {

    val mappingSpecificationFilePath: String = jobProperties.getString("table.mapping_specification.file.path")
    val mappingSpecificationFileSep: String = jobProperties.getString("table.mapping_specification.file.sep")
    val mappingSpecificationFileHeader: Boolean = jobProperties.getBoolean("table.mapping_specification.file.header")
    val mappingSpecificationFileXMLSchemaPath: String = jobProperties.getString("table.mapping_specification.xml.schema.path")

    logger.info(s"Mapping specification file: $mappingSpecificationFilePath")
    logger.info(s"Separator to be used for file reading: $mappingSpecificationFileSep")
    logger.info(s"Does the file has a header? $mappingSpecificationFileHeader")

    logger.info(s"Attempting to load mapping specification file as a spark.sql.DataFrame")

    val mappingSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", mappingSpecificationFilePath)
      .option("sep", mappingSpecificationFileSep)
      .option("header", mappingSpecificationFileHeader)
      .schema(retrieveStructTypeFromXMLFile(mappingSpecificationFileXMLSchemaPath))
      .load()

    logger.info(s"Successfully loaded mapping specification file as a spark.sql.DataFrame")
    writeToJDBC(mappingSpecificationDf, pcAuroraDBName, mappingSpecificationTBLName, SaveMode.Overwrite)
  }

  private def loadLookupTable(): Unit = {

    val lookupSpecificationFilePath: String = jobProperties.getString("table.lookup.file.path")
    val lookupSpecificationFileSep: String = jobProperties.getString("table.lookup.file.sep")
    val lookupSpecificationFileHeader: Boolean = jobProperties.getBoolean("table.lookup.file.header")
    val lookupSpecificationFileXMLSchemaPath: String = jobProperties.getString("table.lookup.xml.schema.path")

    logger.info(s"Lookup specification file: $lookupSpecificationFilePath")
    logger.info(s"Separator to be used for file reading: $lookupSpecificationFileSep")
    logger.info(s"Does the file has a header? $lookupSpecificationFileHeader")

    logger.info(s"Attempting to load lookup specification file as a spark.sql.DataFrame")

    val lookupSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", lookupSpecificationFilePath)
      .option("sep", lookupSpecificationFileSep)
      .option("header", lookupSpecificationFileHeader)
      .schema(retrieveStructTypeFromXMLFile(lookupSpecificationFileXMLSchemaPath))
      .load()

    logger.info(s"Successfully loaded lookup specification file as a spark.sql.DataFrame")
    writeToJDBC(lookupSpecificationDf, pcAuroraDBName, lookupTBLName, SaveMode.Overwrite)
  }
}

