package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

case class GetDfForQuery(override val input: String,
                         override val outputKey: String,
                         private val sqlContext: SQLContext)
  extends IOStep[String, DataFrame](input, "EXECUTE_QUERY", outputKey)
    with Logging {

  override protected def stepFunction(input: String): DataFrame = {

    val output = sqlContext.sql(input)
    log.info(
      s"""Executed query: $input. Retrieved dataframe schema
         |
         |    ${output.schema.treeString}
         |""".stripMargin)
    output
  }
}
