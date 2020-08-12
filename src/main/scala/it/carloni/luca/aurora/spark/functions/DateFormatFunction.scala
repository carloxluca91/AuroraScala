package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}

case class DateFormatFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.dateFormat.regex) {

  private final val inputFormat: String = signatureMatch.group(4)
  private final val outputFormat: String = signatureMatch.group(5)
  logger.info(toString)

  override def toString: String = s"'$functionName($nestedFunctionGroup3, old_format = '$inputFormat', new_format = '$outputFormat')'"

  override def transform(inputColumn: Column): Column = from_unixtime(unix_timestamp(inputColumn, inputFormat), outputFormat)
}
