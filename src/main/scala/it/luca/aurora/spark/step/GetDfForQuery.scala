package it.luca.aurora.spark.step

import it.luca.aurora.core.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

case class GetDfForQuery(private val query: String,
                         private val sqlContext: SQLContext,
                         override val outputKey: String)
  extends IOStep[String, DataFrame]("Execute an HiveQL query", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val output = sqlContext.sql(query)
    log.info(
      s"""Executed query: $query. Retrieved dataframe schema
         |
         |    ${output.schema.treeString}
         |""".stripMargin)
    (outputKey, output)
  }
}
