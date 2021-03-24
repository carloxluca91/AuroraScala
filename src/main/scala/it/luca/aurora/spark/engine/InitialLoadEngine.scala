package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.data.LogRecord
import it.luca.aurora.utils.ColumnName
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.sql._

case class InitialLoadEngine(override protected val sqlContext: SQLContext, 
                             override val propertiesFile: String)
  extends AbstractEngine(sqlContext, propertiesFile) 
    with Logging {
  
  private final val createInitialLoadLogRecord = LogRecord(sqlContext.sparkContext, Branch.InitialLoad.name, None, None,
    _: String, _: String, _: Option[String])

  private final val readTsvAsDataframeAddingVersionNumber: String => DataFrame =
    actualTable =>
      readTsvAsDataframe(actualTable)
        .withColumn(ColumnName.Versione.name, lit("0.1"))

  def run(): Unit = {

    // Create database if it does not exists
    val jdbcConnection: Connection = getJDBCConnection
    createDatabaseIfNotExists(pcAuroraDBName, jdbcConnection)
    jdbcConnection.close()
    info("Successfully closed JDBC connection")

    tableLoadingOptionsMap.keys foreach { key =>

        writeToJDBCAndLog[String](pcAuroraDBName,
          key,
          SaveMode.Append,
          truncateFlag = false,
          createInitialLoadLogRecord,
          readTsvAsDataframeAddingVersionNumber,
          key)
    }
  }

  private def createDatabaseIfNotExists(dbToCreate: String, connection: Connection): Unit = {

    // Extract names of existing databases
    val resultSet: ResultSet = connection.getMetaData.getCatalogs
    val existingDatabases: Seq[String] = Iterator.continually((resultSet.next(), resultSet))
      .takeWhile(_._1)
      .map(_._2.getString("TABLE_CAT"))
      .map(_.toLowerCase)
      .toSeq

    info(s"Existing databases: ${existingDatabases.map(x => s"'$x'").mkString(", ")}")
    val dbToCreateLower: String = dbToCreate.toLowerCase
    if (existingDatabases.contains(dbToCreateLower)) {

      info(s"Database '$dbToCreateLower' already exists. So, not much to do ;)")

    } else {

      val createDbStatement: Statement = connection.createStatement()
      createDbStatement.executeUpdate(s"CREATE DATABASE IF NOT EXISTS $dbToCreateLower")
      info(s"Successfully created database '$dbToCreateLower'")
    }
  }
}

