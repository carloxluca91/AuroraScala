package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.rpad

class Rpad(column: Column, functionToApply: String)
  extends ETLFunction(column, functionToApply, Signature.lpad.signatureRegex) {

  override def transform: Column = {

    val paddingLength: Int = signatureMatch.group(3).toInt
    val paddingString: String = signatureMatch.group(4)

    logger.info(s"function: $functionName, length to pad: $paddingLength, padding charsequence: $paddingString ")
    rpad(nestedFunctionCol, paddingLength, paddingString)

  }
}
