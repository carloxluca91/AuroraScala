package it.luca.aurora.spark.functions.common

import it.luca.aurora.spark.functions.etl.ColumnExpression
import org.apache.spark.sql.Column

abstract class MultipleColumnExpression(override val string: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(string, columnExpression) {

  // TODO: split multiple subexpressions
  final val subExpressions: Seq[String] = group(2).split(", ")

  def getColumn(inputColumns: Column*): Column

}
