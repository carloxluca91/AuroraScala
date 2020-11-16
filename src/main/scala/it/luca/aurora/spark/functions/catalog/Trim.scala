package it.luca.aurora.spark.functions.catalog

import it.luca.aurora.spark.functions.common.ColumnExpression
import it.luca.aurora.spark.functions.common.SingleColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.trim

case class Trim(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.Trim) {

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction)"

  override def getColumn(inputColumn: Column): Column = trim(inputColumn)
}
