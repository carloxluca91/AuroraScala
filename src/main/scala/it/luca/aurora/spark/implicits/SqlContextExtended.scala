package it.luca.aurora.spark.implicits

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{col, lower}

class SqlContextExtended(private val sqlContext: SQLContext) {

  def existsDb(dbName: String): Boolean = {

    sqlContext.sql("SHOW DATABASES")
      .filter(lower(col("databaseName")) === dbName.toLowerCase)
      .count() == 1
  }

  def tableExistsInDb(tableName: String, dbName: String): Boolean = {

    sqlContext.tableNames(dbName)
      .map(_.toLowerCase)
      .contains(tableName.toLowerCase)
  }
}
