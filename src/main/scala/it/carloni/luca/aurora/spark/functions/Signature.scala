package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object Signature extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val dateFormat: Val = Val("^(date_format)\\(((.+),\\s?)'(.+)',\\s?'(.+)'\\)$".r)
  val leftOrRightPad: Val = Val("^([r|l]pad)\\(((.+),\\s)?(\\d+),\\s?'(.+)'\\)$".r)
  val toDateOrTimestamp: Val = Val("^(to_date|to_timestamp)\\(((.+),\\s)?'(.+)'\\)$".r)

}
