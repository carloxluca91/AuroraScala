package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.MultipleColumnFunction
import net.sf.jsqlparser.expression.Function
import org.apache.spark.sql.{Column, functions}

case class Concat(override protected val function: Function)
  extends MultipleColumnFunction(function) {

  override def getColumn(columns: Column*): Column = functions.concat(columns: _*)
}
