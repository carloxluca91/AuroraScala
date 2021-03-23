package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.substring

case class Substring(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.Substring) {

  private val substringStartIndex: Int = group(3).toInt
  private val length: Int = group(4).toInt

  override def getColumn(inputColumn: Column): Column = substring(inputColumn, substringStartIndex, length)

  override def asString: String = s"$functionName($nestedFunction, startIndex = $substringStartIndex, length = $length)"
}
