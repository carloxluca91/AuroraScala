package it.carloni.luca.aurora.spark.engine

import java.io.File

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.util.{Failure, Success, Try}

abstract class AbstractEngine(private final val sparkContext: SparkContext,
                              private final val applicationPropertiesFile: String) {

  private final val logger = Logger.getLogger(getClass)
  protected final val sqlContext: HiveContext = new HiveContext(sparkContext)
  protected final val jobProperties: PropertiesConfiguration = new PropertiesConfiguration()
  loadJobProperties(applicationPropertiesFile)

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

  protected def existsTableInDatabase(tableName: String, databaseName: String): Boolean = sqlContext.tableNames(databaseName).contains(tableName)

}
