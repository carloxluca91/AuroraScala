package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.MultipleColumnFunction
import net.sf.jsqlparser.expression.{Function, StringValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat_ws

case class ConcatWs(override protected val function: Function)
  extends MultipleColumnFunction(function) {

  override def getColumn(columns: Column*): Column = {

    val separator: String = getFunctionParameter[StringValue, String](0, _.getValue)
    concat_ws(separator, columns: _*)
  }
}
