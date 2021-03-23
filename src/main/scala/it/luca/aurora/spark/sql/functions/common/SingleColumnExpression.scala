package it.luca.aurora.spark.sql.functions.common

import org.apache.spark.sql.Column

abstract class SingleColumnExpression(override val stringExpression: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(stringExpression, columnExpression) {

  final val nestedFunction: String = group(2)

  def getColumn(inputColumn: Column): Column

}
