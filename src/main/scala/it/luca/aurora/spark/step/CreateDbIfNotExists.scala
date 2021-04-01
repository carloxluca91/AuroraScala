package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

case class CreateDbIfNotExists(private val dbName: String,
                               private val sqlContext: SQLContext)
  extends IStep[String](stepName = s"CREATE_DB_${dbName.toUpperCase}")
    with Logging {

  override def run(variables: mutable.Map[String, Any]): Unit = {

    if (sqlContext.existsDb(dbName)) {
      log.info(s"Hive DB $dbName already exists")
    } else {
      log.warn(s"Hive DB $dbName does not exist yet. Creating it now")
      sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
      log.info(s"Successfully created Hive DB $dbName")
    }
  }
}
