package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

case class ReadHiveTable(private val tableNameOrInputKey: String,
                         private val isTableName: Boolean,
                         private val sqlContext: SQLContext,
                         override val outputKey: String)
  extends IOStep[String, DataFrame]("Retrieve table from Hive DB", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val tableName = if (isTableName) {
      tableNameOrInputKey
    } else {
      variables(tableNameOrInputKey).asInstanceOf[String]
    }

    val output = sqlContext.table(tableName)
    log.info(
      s"""Read Hive table $tableNameOrInputKey with schema
         |
         |    ${output.schema.treeString}
         |""".stripMargin)
    (outputKey, output)
  }
}
