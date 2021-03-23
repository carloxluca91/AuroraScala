package it.luca.aurora.spark.sql.functions.common

import org.apache.spark.sql.Column

abstract class StaticColumnExpression(override val stringExpression: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(stringExpression, columnExpression) {

  def getColumn: Column

}
