package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object FunctionSignature extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFunctionSignature(x: Value): Val = x.asInstanceOf[Val]

  // val dateFormat: Val = Val("^date_format\\(([\\w*\\-/ :.+\\\\|]+),\\s*([\\w*\\-/ :.+\\\\|]+)\\s*\\)$".r)
  // val lpad: Val = Val("^lpad\\((\\d+),\\s*'(\\w)'\\)$".r)
  // val rpad: Val = Val("^rpad\\((\\d+),\\s*'(\\w)'\\)$".r)

  // REGEX CREATION RULES:
  // group 1 --> FUNCTION NAME
  // group 2 --> POSSIBLE NESTED FUNCTION (marked as optional, i.e. ?). IF NOT PRESENT, THE MATCH WILL EXTRACT null
  // THEN, A GROUP FOR EACH FUNCTION PARAMETER (i.e. group 3 --> first function parameter, group 4 --> second function parameter ...)

  val dateFormat: Val = Val("^(date_format)\\(\\s*(\\w*\\([\\w',.\\-/(): ]+\\))?,?\\s*'([\\w/. ,+\\\\:]+)',\\s*'([\\w/. ,+\\\\:]+)'\\)$".r)
  val lpad: Val = Val("^(lpad)\\(\\s*(\\w*\\([\\w',.\\-/(): ]+\\))?,?\\s*(\\d+),\\s*'(\\w)'\\)$".r)
  val rpad: Val = Val("^(rpad)\\(\\s*(\\w*\\([\\w',.\\-/(): ]+\\))?,?\\s*(\\d+),\\s*'(\\w)'\\)$".r)


}
