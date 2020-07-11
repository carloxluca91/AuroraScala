package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.rpad

class RpadFunction(inputColumn: Column, functionToApply: String)
  extends ETLFunction(inputColumn, functionToApply, Signatures.lpad.regex) {

  private final val logger: Logger = Logger.getLogger(getClass)

  override def transform: Column = {

    val paddingLength: Int = signatureMatch.group(3).toInt
    val paddingString: String = signatureMatch.group(4)

    logger.info(s"function: $functionName, length to pad: $paddingLength, padding charsequence: $paddingString ")
    rpad(nestedFunctionCol, paddingLength, paddingString)
  }
}
