package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object Signature extends Enumeration {

  protected case class Val(signatureRegex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFunctionSignatureVal(x: Value): Val = x.asInstanceOf[Val]

  // REGEX CREATION RULES
  // group 1 --> FUNCTION NAME
  // group 2 --> POSSIBLE NESTED FUNCTION (marked as optional, i.e. ?). IF NOT PRESENT, THE MATCH WILL EXTRACT null
  // THEN, A GROUP FOR EACH FUNCTION PARAMETER (i.e. group 3 --> first function parameter, group 4 --> second function parameter ...)

  val dateFormat: Val = Val("^(date_format)\\((.+\\))?,?\\s?'(.+)',\\s?'(.+)'\\)$".r)
  val lpad: Val = Val("^(lpad)\\((.+\\))?,?\\s?(\\d+),\\s?'(\\w+)'\\)$".r)
  val rpad: Val = Val("^(rpad)\\((.+\\))?,?\\s?(\\d+),\\s?'(\\w+)'\\)$".r)

}