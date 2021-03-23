package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.{ColumnExpression, SingleColumnExpression}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPad(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.LeftOrRightPad) {

  final val length: Int = group(3).toInt
  final val padding: String = group(4)

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction, LENGTH = $length, PADDING = '$padding')"

  override def getColumn(inputColumn: Column): Column = {

    val function: (Column, Int, String) => Column = if (functionName startsWith "l") lpad else rpad
    function(inputColumn, length, padding)
  }
}
