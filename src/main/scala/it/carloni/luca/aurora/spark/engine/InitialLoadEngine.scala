package it.carloni.luca.aurora.spark.engine

import java.sql._

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.utils.{ColumnName, TableId}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractInitialOrReloadEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createInitialLoadLogRecord = createLogRecord(Branch.INITIAL_LOAD.getName, None, None, _: String, _: Option[String])

  def run(): Unit = {

    // CREATE DATABASE, IF IT DOES NOT EXIST
    val jdbcConnection: Connection = getJDBCConnection
    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)
    jdbcConnection.close()
    logger.info("Successfully closed JDBC connection")

    // Function1[String, DataFrame]
    val readTSVAddingVersionNumber: String => DataFrame = tableId =>

      readTSVForTable(tableId)
        .withColumn(ColumnName.VERSIONE.getName, lit(1.0).cast(DataTypes.DoubleType))

    // Map(String -> (String, String, Boolean, String))
    Map(mappingSpecificationTBLName -> TableId.MAPPING_SPECIFICATION.getId,
      lookupTBLName -> TableId.LOOK_UP.getId)
      .foreach(x => {

        val tableName: String = x._1
        val tableId: String = x._2

        writeToJDBCAndLog[String](pcAuroraDBName,
          tableName,
          SaveMode.Append,
          truncateFlag = false,
          createInitialLoadLogRecord,
          readTSVAddingVersionNumber,
          dfGenerationFunctionArg = tableId)
      })
  }

  private def createDatabaseIfNotExists(databaseToCreate: String, connection: Connection): Unit = {

    // RESULT SET CONTAINING DATABASE NAMES
    val resultSet: ResultSet = connection.getMetaData
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
    if (existingDatabases.contains(databaseToCreateLower)) {

      logger.info(s"Database '$databaseToCreateLower' already exists. So, not much to do ;)")

    } else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $databaseToCreateLower")
      logger.info(s"Successfully created database '$databaseToCreateLower'")
    }
  }
}

