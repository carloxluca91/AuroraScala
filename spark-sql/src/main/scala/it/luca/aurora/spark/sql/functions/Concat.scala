package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.MultipleColumnFunction
import net.sf.jsqlparser.expression
import org.apache.spark.sql.{Column, functions}

case class Concat(override val sqlFunction: expression.Function)
  extends MultipleColumnFunction(sqlFunction) {

  override def getColumn(columns: Column*): Column = functions.concat(columns: _*)
}
