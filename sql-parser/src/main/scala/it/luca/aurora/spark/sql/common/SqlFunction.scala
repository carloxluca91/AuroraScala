package it.luca.aurora.spark.sql.common

import net.sf.jsqlparser.expression._

abstract class SqlFunction(protected val function: Function) {

  protected final val functionName: String = function.getName
  protected final def getFunctionParameter[R <: Expression, T](index: Int, rToT: R => T) :T = {
    rToT(function.getParameters
      .getExpressions
      .get(index).asInstanceOf[R])
  }
}
