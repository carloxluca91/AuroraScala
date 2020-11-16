package it.luca.aurora.spark.engine

import java.sql._

import it.luca.aurora.option.Branch
import it.luca.aurora.utils.{ColumnName, TableId}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractInitialOrReloadEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createInitialLoadLogRecord = createLogRecord(Branch.InitialLoad.name, None, None, _: String, _: String, _: Option[String])

  def run(): Unit = {

    // Create database if it does not exists
    val jdbcConnection: Connection = getJDBCConnection
    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)
    jdbcConnection.close()
    logger.info("Successfully closed JDBC connection")

    val readTsvAsDataframeAddingVersionNumber: String => DataFrame = tableId =>

      readTsvAsDataframe(tableId)
        .withColumn(ColumnName.Versione.name, lit("0.1"))

    // Write Mapping specification table
    writeToJDBCAndLog[String](pcAuroraDBName,
      mappingSpecificationTBLName,
      SaveMode.Append,
      truncateFlag = false,
      createInitialLoadLogRecord,
      readTsvAsDataframeAddingVersionNumber,
      TableId.MappingSpecification.tableId)

    // Write Lookup table
    writeToJDBCAndLog[String](pcAuroraDBName,
      lookupTBLName,
      SaveMode.Append,
      truncateFlag = false,
      createInitialLoadLogRecord,
      readTsvAsDataframeAddingVersionNumber,
      TableId.Lookup.tableId)
  }

  private def createDatabaseIfNotExists(dbToCreate: String, connection: Connection): Unit = {

    // Extract names of existing databases
    val resultSet: ResultSet = connection.getMetaData.getCatalogs
    val existingDatabases: Seq[String] = Iterator.continually((resultSet.next(), resultSet))
      .takeWhile(_._1)
      .map(_._2.getString("TABLE_CAT"))
      .map(_.toLowerCase)
      .toSeq

    logger.info(s"Existing databases: ${existingDatabases.map(x => s"'$x'").mkString(", ")}")
    val dbToCreateLower: String = dbToCreate.toLowerCase
    if (existingDatabases.contains(dbToCreateLower)) {

      logger.info(s"Database '$dbToCreateLower' already exists. So, not much to do ;)")

    } else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $dbToCreateLower")
      logger.info(s"Successfully created database '$dbToCreateLower'")
    }
  }
}

