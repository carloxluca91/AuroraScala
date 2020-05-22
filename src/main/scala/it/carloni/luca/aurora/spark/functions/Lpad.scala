package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lpad

class Lpad(column: Column, functionToApply: String)
  extends ETLFunction(column, functionToApply, Signature.lpad.signatureRegex) {

  override def transform: Column = {

    val paddingLength: Int = matcher.group(3).toInt
    val paddingString: String = matcher.group(4)

    logger.info(s"function: $functionName, length to pad: $paddingLength, padding charsequence: $paddingString ")
    lpad(nestedFunctionCol, paddingLength, paddingString)

  }
}
