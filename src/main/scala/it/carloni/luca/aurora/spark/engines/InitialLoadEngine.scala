package it.carloni.luca.aurora.spark.engines

import java.sql.{Connection, DatabaseMetaData, DriverManager, ResultSet, Statement}

import org.apache.log4j.Logger

class InitialLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(): Unit = {

    val jdbcUrlUseSSLConnectionString: String = s"$jdbcUrl/?useSSL=$jdbcUseSSL"

    logger.info(s"Attempting to connect to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")

    val jdbcConnection: Connection = DriverManager.getConnection(jdbcUrlUseSSLConnectionString, jdbcUser, jdbcPassword)

    logger.info(s"Successfully connected to JDBC url $jdbcUrlUseSSLConnectionString with credentials ($jdbcUser, $jdbcPassword)")
    val databaseMetaData: DatabaseMetaData = jdbcConnection.getMetaData

    val resultSet: ResultSet = databaseMetaData.getCatalogs
    val existingDatabases: Seq[String] = Iterator.continually((resultSet.next(), resultSet))
      .takeWhile(_._1)
      .map(_._2.getString("TABLE_CAT"))
      .map(_.toUpperCase)
      .toSeq

    logger.info(s"Existing databases: ${existingDatabases.mkString(", ")}")
    createDatabaseIfNotExists(lakeCedacriDBName, existingDatabases, jdbcConnection)
    createDatabaseIfNotExists(pcAuroraDBName, existingDatabases, jdbcConnection)
  }

  private def createDatabaseIfNotExists(databaseToCreate: String, existingDatabases: Seq[String], connection: Connection): Unit = {

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
}
