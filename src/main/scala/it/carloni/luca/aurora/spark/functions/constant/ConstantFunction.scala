package it.carloni.luca.aurora.spark.functions.constant

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ConstantFunction(functionToApply: String, signature: Regex) {

  protected final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)

  def getColumn: Column

}
