package it.luca.aurora.spark.functions.common

import scala.util.matching.Regex

object ColumnExpression extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)

}
