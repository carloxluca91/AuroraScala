package it.luca.aurora.spark.sql.functions.common

import scala.util.matching.Regex

object ColumnExpression extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Cast: Val = Val("^(cast)\\((\\w+\\(.*\\)), '(\\w+)'\\)$".r)
  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val LeftOrRightPad: Val = Val("^([l|r]pad)\\((\\w+\\(.*\\)), (\\d+), '(.+)'\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)
  val LowerOrUpper: Val = Val("^(lower|upper)\\((\\w+\\(.*\\))\\)$".r)
  val OrElse: Val = Val("^(or_else)\\((\\w+\\(.*\\)), ('?.+'?)\\)$".r)
  val Replace: Val = Val("^(replace)\\((.+\\)), '(.+)', '(.+)'\\)$".r)
  val Substring: Val = Val("^(substring)\\((\\w+\\(.*\\)), (\\d+), (\\d+)\\)$".r)
  val ToDateOrTimestamp: Val = Val("^(to_date|to_timestamp)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)
  val Trim: Val = Val("^(trim)\\((\\w+\\(.*\\))\\)$".r)

}
