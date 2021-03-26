package it.luca.aurora.spark.sql.common

import net.sf.jsqlparser.expression
import org.apache.spark.sql.Column

abstract class OneColumnFunction(override val sqlFunction: expression.Function)
  extends SqlFunction(sqlFunction) {

  def getColumn(column: Column): Column
}
