package it.luca.aurora.spark.step

import it.luca.aurora.core.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

case class CreateDbIfNotExists(private val dbName: String,
                               private val sqlContext: SQLContext)
  extends IStep[String](s"Create Hive DB $dbName")
    with Logging {

  override def run(variables: mutable.Map[String, Any]): Unit = sqlContext.createDbIfNotExists(dbName)
}
