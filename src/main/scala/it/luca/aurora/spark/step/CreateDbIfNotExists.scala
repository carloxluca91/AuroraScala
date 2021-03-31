package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.SQLContext

case class CreateDbIfNotExists(override val input: String, private val sqlContext: SQLContext)
  extends IStep[String](input, stepName = s"CREATE_DB_${input.toUpperCase}")
    with Logging {

  override def run(): Unit = {

    if (sqlContext.existsDb(input)) {
      log.info(s"Hive DB $input already exists")
    } else {
      log.warn(s"Hive DB $input does not exist yet. Creating it now")
      sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $input")
      log.info(s"Successfully created Hive DB $input")
    }
  }
}
