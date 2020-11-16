package it.luca.aurora.spark.functions.common

import it.luca.aurora.spark.functions.etl.ColumnExpression

abstract class AbstractExpression(val string: String, val columnExpression: ColumnExpression.Value) {

  private final val expressionMatch: scala.util.matching.Regex.Match = columnExpression.regex.findFirstMatchIn(string).get
  protected final val functionName: String = expressionMatch.group(1)

  def group(i: Int): String = expressionMatch.group(i)

  def asString: String

}
