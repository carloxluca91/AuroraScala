package it.luca.aurora.spark.functions.catalog

import it.luca.aurora.spark.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lower, upper}

case class LowerOrUpper(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.LowerOrUpper) {

  override def getColumn(inputColumn: Column): Column = {

    val function: Column => Column = if (functionName.toLowerCase startsWith "lower") lower else upper
    function(inputColumn)
  }

  override def asString: String = s"$functionName($nestedFunction)"
}
