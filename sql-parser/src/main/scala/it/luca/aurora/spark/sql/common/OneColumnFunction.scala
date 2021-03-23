package it.luca.aurora.spark.sql.common

import net.sf.jsqlparser.expression.Function
import org.apache.spark.sql.Column

abstract class OneColumnFunction(override protected val function: Function)
  extends SqlFunction(function) {

  def getColumn(column: Column): Column
}
