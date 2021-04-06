package it.luca.aurora.spark.implicits

import it.luca.aurora.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, lower}

class SqlContextExtended(private val sqlContext: SQLContext)
  extends Logging {

  def createDbIfNotExists(dbName: String): Unit = {

    if (sqlContext.existsDb(dbName)) {
      log.info(s"Hive DB $dbName already exists")
    } else {
      log.warn(s"Hive DB $dbName does not exist yet. Creating it now")
      sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
      log.info(s"Successfully created Hive DB $dbName")
    }
  }

  def existsDb(dbName: String): Boolean = {

    sqlContext.sql("SHOW DATABASES")
      .filter(lower(col("result")) === dbName.toLowerCase)
      .count() == 1
  }

  def existsTable(tableName: String, dbName: String): Boolean = {

    sqlContext.tableNames(dbName)
      .map(_.toLowerCase)
      .contains(tableName.toLowerCase)
  }
}
