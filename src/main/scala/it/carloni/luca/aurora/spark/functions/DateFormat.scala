package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}

class DateFormat(column: Column, functionToApply: String)
  extends ETLFunction(column, functionToApply, Signature.dateFormat.signatureRegex) {

  override def transform: Column = {

    val inputFormat: String = matcher.group(3)
    val outputFormat: String = matcher.group(4)

    logger.info(s"function: $functionName, Input format: $inputFormat, Output format: $outputFormat")
    from_unixtime(unix_timestamp(nestedFunctionCol, inputFormat), outputFormat)

  }
}
