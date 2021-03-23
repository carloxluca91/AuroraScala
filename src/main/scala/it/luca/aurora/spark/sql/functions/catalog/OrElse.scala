package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{coalesce, lit}

case class OrElse(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.OrElse) {

  private val orElseValue: String = group(3)
  override def getColumn(inputColumn: Column): Column = {

    val orElseFinalValue: String = if (orElseValue.startsWith("'") && orElseValue.endsWith("'")) {
      orElseValue.substring(1, orElseValue.length - 1)
    } else {
      orElseValue
    }

    coalesce(inputColumn, lit(orElseFinalValue))
  }

  override def asString: String = s"$functionName($nestedFunction, lit($orElseValue))"
}
