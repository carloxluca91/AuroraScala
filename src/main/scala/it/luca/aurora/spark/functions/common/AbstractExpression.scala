package it.luca.aurora.spark.functions.common

abstract class AbstractExpression(val stringExpression: String, val columnExpression: ColumnExpression.Value) {

  private final val expressionMatch: scala.util.matching.Regex.Match = columnExpression.regex.findFirstMatchIn(stringExpression).get
  protected final val functionName: String = expressionMatch.group(1)

  def group(i: Int): String = expressionMatch.group(i)

  def asString: String

}
