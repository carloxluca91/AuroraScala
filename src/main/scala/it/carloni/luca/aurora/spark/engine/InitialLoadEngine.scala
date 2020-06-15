package it.carloni.luca.aurora.spark.engine

import java.sql._

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

    createDatabasesIfNotExist(Seq(lakeCedacriDBName, pcAuroraDBName), jdbcConnection)
    Try(createMappingSpecificationTable()) match {

      case Failure(exception) =>

        logger.error(s"Error while trying to load table $pcAuroraDBName.$mappingSpecificationTBLName")
        logger.error(s"Message: ${exception.getMessage}")
        exception.printStackTrace()

      case Success(_) =>

        logger.info(s"Successfully loaded table $pcAuroraDBName.$mappingSpecificationTBLName")
    }

    Try(createLookupTable()) match {

      case Failure(exception) =>

        logger.error(s"Error while trying to load table $pcAuroraDBName.$lookupSpecificationTBLName")
        logger.error(s"Message: ${exception.getMessage}")
        exception.printStackTrace()

      case Success(_) =>

        logger.info(s"Successfully loaded table $pcAuroraDBName.$lookupSpecificationTBLName")
    }

    logger.info("Attempting to close JDBC connection")
    jdbcConnection.close()
    logger.info("Successfully closed JDBC connection")
  }

  private def createDatabasesIfNotExist(databasesToCreate: Seq[String], connection: Connection): Unit = {

    def createDatabaseIfNotExists(databaseToCreate: String, existingDatabases: Seq[String], connection: Connection): Unit = {

      val databaseToCreateUpper: String = databaseToCreate.toUpperCase
      if (existingDatabases.contains(databaseToCreateUpper)) {

        logger.info(s"Database $databaseToCreateUpper already exists, so it will not be recreated")
      }

      else {

        val createDbStatement: Statement = connection.createStatement()
        createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseToCreate")
        logger.info(s"Successfully created database $databaseToCreate")
      }
    }

    // RESULT SET CONTAINING DATABASE NAMES
    val resultSet: ResultSet = connection
      .getMetaData
      .getCatalogs

    // EXTRACT THOSE NAMES
    val existingDatabases: Seq[String] = Iterator.continually((resultSet.next(), resultSet))
      .takeWhile(_._1)
      .map(_._2.getString("TABLE_CAT"))
      .map(_.toUpperCase)
      .toSeq

    logger.info(s"Existing databases: ${existingDatabases.mkString(", ")}")

    databasesToCreate
      .foreach(createDatabaseIfNotExists(_, existingDatabases, connection))
  }

  private def createMappingSpecificationTable(): Unit = {

    val mappingSpecificationFilePath: String = jobProperties.getString("table.mapping_specification.file.path")
    val mappingSpecificationFileSep: String = jobProperties.getString("table.mapping_specification.file.sep")
    val mappingSpecificationFileHeader: Boolean = jobProperties.getBoolean("table.mapping_specification.file.header")
    val mappingSpecificationFileSchema: String = jobProperties.getString("table.mapping_specification.schema")

    logger.info(s"Mapping specification file: $mappingSpecificationFilePath")
    logger.info(s"Separator to be used for file reading: $mappingSpecificationFileSep")
    logger.info(s"Does the file has a header? $mappingSpecificationFileHeader")

    logger.info(s"Attempting to load mapping specification file as a spark.sql.DataFrame")

    val mappingSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", mappingSpecificationFilePath)
      .option("sep", mappingSpecificationFileSep)
      .option("header", mappingSpecificationFileHeader)
      .schema(retrieveStructTypeFromString(mappingSpecificationFileSchema))
      .load()

    logger.info(s"Successfully loaded mapping specification file as a spark.sql.DataFrame")
    mappingSpecificationDf.printSchema()
    writeToJDBC(mappingSpecificationDf, pcAuroraDBName, mappingSpecificationTBLName, SaveMode.Overwrite)
  }

  private def createLookupTable(): Unit = {

    val lookupSpecificationFilePath: String = jobProperties.getString("table.lookup.file.path")
    val lookupSpecificationFileSep: String = jobProperties.getString("table.lookup.file.sep")
    val lookupSpecificationFileHeader: Boolean = jobProperties.getBoolean("table.lookup.file.header")
    val lookupSpecificationFileSchema: String = jobProperties.getString("table.lookup.schema")

    logger.info(s"Lookup specification file: $lookupSpecificationFilePath")
    logger.info(s"Separator to be used for file reading: $lookupSpecificationFileSep")
    logger.info(s"Does the file has a header? $lookupSpecificationFileHeader")

    logger.info(s"Attempting to load lookup specification file as a spark.sql.DataFrame")

    val lookupSpecificationDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", lookupSpecificationFilePath)
      .option("sep", lookupSpecificationFileSep)
      .option("header", lookupSpecificationFileHeader)
      .schema(retrieveStructTypeFromString(lookupSpecificationFileSchema))
      .load()

    logger.info(s"Successfully loaded lookup specification file as a spark.sql.DataFrame")
    lookupSpecificationDf.printSchema()
    writeToJDBC(lookupSpecificationDf, pcAuroraDBName, lookupSpecificationTBLName, SaveMode.Overwrite)
  }
}
