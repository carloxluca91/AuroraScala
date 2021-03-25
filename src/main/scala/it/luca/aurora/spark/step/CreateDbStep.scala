package it.luca.aurora.spark.step

import grizzled.slf4j.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.SQLContext

case class CreateDbStep(override protected val input: String, private val sqlContext: SQLContext)
  extends IStep[String](input, s"CREATE_DB_$input")
    with Logging {

  override def run(): Unit = {

    if (sqlContext.existsDb(input)) {
      info(s"Hive DB $input already exists")
    } else {
      warn(s"Hive DB $input does not exist yet. Creating it now")
      sqlContext.sql(s"CREATE DATABASE IF NOT EXISTS $input")
      info(s"Successfully created Hive DB $input")
    }
  }
}
