package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object Signature extends Enumeration {

  protected case class Val(signatureRegex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFunctionSignatureVal(x: Value): Val = x.asInstanceOf[Val]

  val dateFormat: Val = Val("^(date_format)\\(((.+),\\s)?'(.+)',\\s'(.+)'\\)$".r)
  val lookUpIstituto: Val = Val("^(look_up_istituto)\\((.+)?\\)".r)
  val lpad: Val = Val("^(lpad)\\(((.+),\\s)?(\\d+),\\s'(.+)'\\)$".r)
  val rpad: Val = Val("^(rpad)\\(((.+),\\s)?(\\d+),\\s'(.+)'\\)$".r)
  val toDate: Val = Val("^(to_date)\\(((.+),\\s)?'(.+)'\\)$".r)
  val toTimestamp: Val = Val("^(to_timestamp)\\(((.+),\\s)?'(.+)'\\)$".r)

}
