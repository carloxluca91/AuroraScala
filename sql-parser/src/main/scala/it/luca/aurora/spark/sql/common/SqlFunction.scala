package it.luca.aurora.spark.sql.common

import net.sf.jsqlparser.expression
import net.sf.jsqlparser.expression.Expression

abstract class SqlFunction(val sqlFunction: expression.Function) {

  protected final val functionName: String = sqlFunction.getName
  protected final def getFunctionParameter[R <: Expression, T](index: Int, rToT: R => T) :T = {
    rToT(sqlFunction.getParameters
      .getExpressions
      .get(index).asInstanceOf[R])
  }
}
