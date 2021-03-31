package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

case class ReadHiveTable(override val input: String,
                         override val outputKey: String,
                         private val sqlContext: SQLContext)
  extends IOStep[String, DataFrame](input, outputKey, s"READ_HIVE_TABLE_${input.toUpperCase}")
    with Logging {

  override protected def stepFunction(input: String): DataFrame = {

    val output = sqlContext.table(input)
    log.info(
      s"""Read Hive table $input with schema
         |
         |    ${output.schema.treeString}
         |""".stripMargin)
    output
  }
}
