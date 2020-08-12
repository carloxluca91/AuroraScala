package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ETLFunction(functionToApply: String, signature: Regex) {

  protected final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)
  val nestedFunctionGroup3: String = signatureMatch.group(3)

  def hasNestedFunction: Boolean = !nestedFunctionGroup3.equalsIgnoreCase("@")

  def transform(inputColumn: Column): Column
}
