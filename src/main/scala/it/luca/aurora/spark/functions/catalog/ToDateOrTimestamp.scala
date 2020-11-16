package it.luca.aurora.spark.functions.catalog

import it.luca.aurora.spark.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestamp(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.ToDateOrTimestamp) {

  final val format: String = group(3)

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction, FORMAT = '$format')"

  override def getColumn(inputColumn: Column): Column = {

    val function: (Column, String) => Column = if (functionName endsWith "date") to_date else to_timestamp
    function(inputColumn, format)
  }
}
