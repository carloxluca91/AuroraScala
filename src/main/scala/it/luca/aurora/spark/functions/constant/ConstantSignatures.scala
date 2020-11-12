package it.luca.aurora.spark.functions.constant

import scala.util.matching.Regex

object ConstantSignatures extends Enumeration {

  protected case class Val(regex: Regex) extends super.Val

  import scala.language.implicitConversions

  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val nowAsDateOrTimestamp: Val = Val("^(now_as_date|now_as_timestamp)\\(\\)$".r)

}
