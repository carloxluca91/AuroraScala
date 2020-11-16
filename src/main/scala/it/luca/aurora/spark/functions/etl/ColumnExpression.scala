package it.luca.aurora.spark.functions.etl

import scala.util.matching.Regex

object ColumnExpression extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  // NEW ONES
  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)

  // OLD ONES
  val dateFormat: Val = Val("^(date_format)\\(((.+),\\s?)'(.+)',\\s?'(.+)'\\)$".r)
  val leftOrRightPad: Val = Val("^([r|l]pad)\\(((.+),\\s?)(\\d+),\\s?'(.+)'\\)$".r)
  val leftOrRightConcat: Val = Val("^([r|l]concat)\\(((.+),\\s)((.+\\()?col\\('\\w+'\\),\\s[^)]+\\)|col\\('\\w+'\\)|lit\\('[\\w|\\s]+'\\))\\)$".r)
  val leftOrRightConcatWs: Val = Val("^([r|l]concat_ws)\\(((.+),\\s)((.+\\()?col\\('\\w+'\\),\\s[^)]+\\)|col\\('\\w+'\\)|lit\\('[\\w|\\s]+'\\)),\\s'(\\w|\\s)+'\\)$".r)
  val toDateOrTimestamp: Val = Val("^(to_date|to_timestamp)\\(((.+),\\s?)'(.+)'\\)$".r)
  val toDateY2: Val = Val("^(to_date_Y2)\\(((.+),\\s)'(.+)'\\)$".r)

}
