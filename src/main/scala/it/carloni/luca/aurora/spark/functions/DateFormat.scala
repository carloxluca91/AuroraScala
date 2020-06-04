package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}

class DateFormat(column: Column, functionToApply: String)
  extends ETLFunction(column, functionToApply, Signature.dateFormat.signatureRegex) {

  private final val logger: Logger = Logger.getLogger(getClass)

  override def transform: Column = {

    val inputFormat: String = signatureMatch.group(3)
    val outputFormat: String = signatureMatch.group(4)

    logger.info(s"Function: $functionName, Input format: $inputFormat, Output format: $outputFormat")
    from_unixtime(unix_timestamp(nestedFunctionCol, inputFormat), outputFormat)

  }
}
