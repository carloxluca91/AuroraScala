package it.luca.aurora.spark.sql.common

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.Expression
import org.apache.spark.sql.Column

sealed abstract class SqlFunction(val sqlFunction: expression.Function) {

  protected final val functionName: String = sqlFunction.getName
  protected final def getFunctionParameter[R <: Expression, T](index: Int, rToT: R => T) :T = {
    rToT(sqlFunction.getParameters
      .getExpressions
      .get(index).asInstanceOf[R])
  }
}

abstract class OneColumnFunction(override val sqlFunction: expression.Function)
  extends SqlFunction(sqlFunction) {

  def getColumn(column: Column): Column
}

abstract class MultipleColumnFunction(override val sqlFunction: expression.Function)
  extends SqlFunction(sqlFunction) {

  def getColumn(columns: Column*): Column

}