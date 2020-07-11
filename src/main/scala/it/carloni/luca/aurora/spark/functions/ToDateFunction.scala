package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.to_date

class ToDateFunction(inputColumn: Column, functionToApply: String)
  extends ETLFunction(inputColumn, functionToApply, Signatures.toTimestamp.regex) {

  private final val logger: Logger = Logger.getLogger(getClass)

  override def transform: Column = {

    val inputFormat: String = signatureMatch.group(4)

    logger.info(s"Function: $functionName, Input format: $inputFormat")
    to_date(nestedFunctionCol, inputFormat)
  }
}