package it.carloni.luca.aurora.spark.engine

import java.io.File

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, DataFrameReader, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

abstract class AbstractEngine(private final val applicationPropertiesFile: String) {

  private final val logger = Logger.getLogger(getClass)

  logger.info("Trying to create SparkSession")

  protected final val sparkSession: SparkSession = SparkSession.builder().getOrCreate()
  protected final val jobProperties: PropertiesConfiguration = new PropertiesConfiguration()
  loadJobProperties(applicationPropertiesFile)

  logger.info(s"Successfully created SparkSession for application ${sparkSession.sparkContext.appName}")
  logger.info(s"Spark application UI url: ${sparkSession.sparkContext.uiWebUrl.get}")

  // JDBC SETTINGS
  private final val jdbcUrl: String = jobProperties.getString("jdbc.url")
  private final val jdbcDriverClassName: String = jobProperties.getString("jdbc.driver.className")
  private final val jdbcUser: String = jobProperties.getString("jdbc.user")
  private final val jdbcPassword: String = jobProperties.getString("jdbc.password")

  // JDBC DATAFRAME READER
  private final val jdbcReader: DataFrameReader = sparkSession.read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("driver", jdbcDriverClassName)
    .option("user", jdbcUser)
    .option("password", jdbcPassword)

  // DATABASES
  protected final val pcAuroraDBName: String = jobProperties.getString("database.pc_aurora.name")
  protected final val lakeCedacriDBName: String = jobProperties.getString("database.lake_cedacri.name")

  // TABLES
  protected final val mappingSpecificationTBLName: String = jobProperties.getString("table.mapping_specification.name")
  protected final val mappingSpecificationFullTBLName: String = s"$pcAuroraDBName.$mappingSpecificationTBLName"

  protected final val dataLoadLogTBLName: String = jobProperties.getString("table.dataload_log.name")
  protected final val dataLoadLogFullTBLName: String = s"$pcAuroraDBName.$dataLoadLogTBLName"

  private def loadJobProperties(propertiesFile: String): Unit = {

    val loadPropertiesTry: Try[Unit] = Try(jobProperties.load(new File(propertiesFile)))
    loadPropertiesTry match {

      case Failure(exception) =>

        logger.error("Exception occurred while loading properties file")
        logger.error(exception)
        throw exception

      case Success(_) => logger.info("Successfully loaded properties file")
    }
  }

  protected def readFromJDBC(databaseName: String, tableName: String): DataFrame = {

    val fullTableName: String = s"$databaseName.$tableName"

    logger.info(s"Starting to load JDBC table $fullTableName")

    val tryLoadJDBCDf: Try[DataFrame] = Try(jdbcReader
      .option("dbtable", fullTableName)
      .load())

    tryLoadJDBCDf match {

      case Failure(exception) =>

        logger.error(s"Error while trying to load JDBC table $fullTableName. Rationale: ${exception.getMessage}")
        exception.printStackTrace()
        throw exception

      case Success(value) =>

        logger.info(s"Successfully loaded JDBC table $fullTableName")
        value
    }
  }

  protected def writeToJDBC(outputDataFrame: DataFrame, databaseName: String, tableName: String, saveMode: SaveMode): Unit = {

    val fullTableName: String = s"$databaseName.$tableName"

    logger.info(s"Starting to save dataframe into JDBC table $fullTableName with savemode $saveMode")

    val tryWriteDfToJDBC: Try[Unit] = Try(outputDataFrame.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", jdbcDriverClassName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("dbtable", fullTableName)
      .mode(saveMode)
      .save())

    tryWriteDfToJDBC match {

      case Failure(exception) =>

        logger.error(s"Error while trying to write dataframe to JDBC table $fullTableName with savemode $saveMode. Rationale: ${exception.getMessage}")
        exception.printStackTrace()
        throw exception

      case Success(_) =>

        logger.info(s"Successfully saved dataframe into JDBC table $fullTableName with savemode $saveMode")
    }
  }
}
