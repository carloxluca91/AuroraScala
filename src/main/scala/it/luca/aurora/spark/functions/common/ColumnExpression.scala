package it.luca.aurora.spark.functions.common

import scala.util.matching.Regex

object ColumnExpression extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Cast: Val = Val("^(cast)\\((\\w+\\(.*\\)), '(\\w+)'\\)$".r)
  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val LeftOrRightPad: Val = Val("^([l|r]pad)\\((\\w+\\(.*\\)), (\\d+), '(.+)'\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)
  val Replace: Val = Val("^(replace)\\((.+\\)), '(.+)', '(.+)'\\)$".r)
  val ToDateOrTimestamp: Val = Val("^(to_date|to_timestamp)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)
  val Trim: Val = Val("^(trim)\\((\\w+\\(.*\\))\\)$".r)

}
