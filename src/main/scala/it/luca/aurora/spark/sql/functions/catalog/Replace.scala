package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.regexp_replace

case class Replace(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.Replace) {

  final val pattern: String = group(3)
  final val replacement: String = group(4)

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction, PATTERN = '$pattern', REPLACEMENT = '$replacement')"

  override def getColumn(inputColumn: Column): Column = regexp_replace(inputColumn, pattern, replacement)
}
