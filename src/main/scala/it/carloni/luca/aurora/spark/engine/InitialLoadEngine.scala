package it.carloni.luca.aurora.spark.engine

import java.sql._

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.spark.data.LogRecord
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createInitialLoadLogRecord = createLogRecord(Branch.INITIAL_LOAD.getName, None, None, _: String, _: Option[String])

  // JDBC SETTINGS
  private final val jdbcURL: String = jobProperties.getString("jdbc.url")
  private final val jdbcUser: String = jobProperties.getString("jdbc.user")
  private final val jdbcPassword: String = jobProperties.getString("jdbc.password")
  private final val jdbcUseSSL: String = jobProperties.getString("jdbc.useSSL")

  def run(): Unit = {

    // CREATE DATABASE, IF IT DOES NOT EXIST
    Class.forName("com.mysql.jdbc.Driver")

    val jdbcUrlConnectionStr: String = s"$jdbcURL/?useSSL=$jdbcUseSSL"
    logger.info(s"Attempting to connect to JDBC url $jdbcUrlConnectionStr with credentials ($jdbcUser, $jdbcPassword)")

    val jdbcConnection: Connection = DriverManager.getConnection(jdbcUrlConnectionStr,
      jobProperties.getString("jdbc.user"),
      jobProperties.getString("jdbc.password"))

    logger.info(s"Successfully connected to JDBC url $jdbcUrlConnectionStr with credentials ($jdbcUser, $jdbcPassword)")
    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)
    logger.info("Attempting to close JDBC connection")

    jdbcConnection.close()
    logger.info("Successfully closed JDBC connection")

    // TABLE LOADING
    val stringToDataFrameMap: Map[String, DataFrame] = Map(

      mappingSpecificationTBLName -> super.getMappingSpecificationDf
        .withColumn("versione", lit(1.0)),

      lookupTBLName -> super.getLookUpDf
        .withColumn("versione", lit(1.0))
    )

    // FOR EACH TABLE:
    // [a] TRY TO EXECUTE THE RELATED LOADING PROCESS
    // [b] DEFINE THE RELATED LOG RECORD
    val initialLoadLogRecords: Seq[LogRecord] = (for ((tableName, dataFrame) <- stringToDataFrameMap) yield {

      val functionExceptionMsgOpt: Option[String] = Try(writeToJDBC(dataFrame, pcAuroraDBName, tableName, SaveMode.Append)) match {

        case Failure(exception) => Some(exception.getMessage)
        case Success(_) => None
      }

      createInitialLoadLogRecord(tableName, functionExceptionMsgOpt)
    }).toSeq

    writeLogRecords(initialLoadLogRecords)
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
      .map(x => s"'$x'")
      .mkString(", ")}")

    val databaseToCreateLower: String = databaseToCreate.toLowerCase
    if (existingDatabases.contains(databaseToCreateLower))

      logger.info(s"Database '$databaseToCreateLower' already exists. So, not much to do ;)")

    else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseToCreateLower")
      logger.info(s"Successfully created database '$databaseToCreateLower'")
    }
  }
}

