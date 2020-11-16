package it.luca.aurora.spark.functions.common

import org.apache.spark.sql.Column

abstract class MultipleColumnExpression(override val stringExpression: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(stringExpression, columnExpression) {

  // TODO: split multiple subexpressions
  final val subExpressions: Seq[String] = group(2).split(", ")

  def getColumn(inputColumns: Column*): Column

}
