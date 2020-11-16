package it.luca.aurora.spark.functions.common

import it.luca.aurora.spark.functions.etl.ColumnExpression
import org.apache.spark.sql.Column

abstract class StaticColumnExpression(override val string: String, override val columnExpression: ColumnExpression.Value)
  extends AbstractExpression(string, columnExpression) {

  def getColumn: Column

}
