package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.OneColumnFunction
import net.sf.jsqlparser.expression.{Function, LongValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.substring

case class Substring(override val sqlFunction: Function)
  extends OneColumnFunction(sqlFunction) {

  override def getColumn(column: Column): Column = {

    val start: Int = getFunctionParameter[LongValue, Int](1, _.getValue.toInt)
    val end: Int = getFunctionParameter[LongValue, Int](2, _.getValue.toInt)
    substring(column, start, end)
  }
}
