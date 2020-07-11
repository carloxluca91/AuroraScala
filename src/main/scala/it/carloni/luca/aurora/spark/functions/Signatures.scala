package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object Signatures extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFunctionSignatureVal(x: Value): Val = x.asInstanceOf[Val]

  val dateFormat: Val = Val("^(date_format)\\(((.+),\\s)?'(.+)',\\s'(.+)'\\)$".r)
  val lookUp: Val = Val("^(look_up)\\(((.+),\\s)?'(.+)',\\s'(.+)'\\)$".r)
  val lpad: Val = Val("^(lpad)\\(((.+),\\s)?(\\d+),\\s'(.+)'\\)$".r)
  val rpad: Val = Val("^(rpad)\\(((.+),\\s)?(\\d+),\\s'(.+)'\\)$".r)
  val toDate: Val = Val("^(to_date)\\(((.+),\\s)?'(.+)'\\)$".r)
  val toTimestamp: Val = Val("^(to_timestamp)\\(((.+),\\s)?'(.+)'\\)$".r)

}
