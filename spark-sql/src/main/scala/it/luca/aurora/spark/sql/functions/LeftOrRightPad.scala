package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.OneColumnFunction
import net.sf.jsqlparser.expression.{Function, LongValue, StringValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPad(override val sqlFunction: Function)
  extends OneColumnFunction(sqlFunction) {

  override def getColumn(column: Column): Column = {

    val length: Int = getFunctionParameter[LongValue, Int](1, _.getValue.toInt)
    val padding: String = getFunctionParameter[StringValue, String](2, _.getValue)
    val paddingFunction: (Column, Int, String) => Column = if (functionName.toLowerCase.startsWith("l")) lpad else rpad
    paddingFunction(column, length, padding)
  }
}
