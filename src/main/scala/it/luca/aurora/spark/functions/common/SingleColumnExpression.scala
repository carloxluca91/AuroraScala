package it.luca.aurora.spark.functions.common

import it.luca.aurora.spark.functions.etl.ColumnExpression
import org.apache.spark.sql.Column

abstract class SingleColumnExpression(override val string: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(string, columnExpression) {

  final val nestedFunction: String = group(2)

  def getColumn(inputColumn: Column): Column

}
