package it.carloni.luca.aurora.spark.functions

import scala.util.matching.Regex

object FunctionSignature extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToFunctionSignature(x: Value): Val = x.asInstanceOf[Val]

  val dateFormat: Val = Val("^date_format\\(([\\w*\\-/ :.+\\\\|]+),\\s*([\\w*\\-/ :.+\\\\|]+)\\s*\\)$".r)
  val lpad: Val = Val("^lpad\\((\\d+),\\s*'(\\w)'\\)$".r)
  val rpad: Val = Val("^rpad\\((\\d+),\\s*'(\\w)'\\)$".r)

}
