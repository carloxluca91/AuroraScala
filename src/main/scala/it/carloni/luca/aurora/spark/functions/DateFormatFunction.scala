package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.date_format

case class DateFormatFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.dateFormat.regex) {

  private final val inputFormat: String = signatureMatch.group(4)
  private final val outputFormat: String = signatureMatch.group(5)

  logger.info(toString)

  override def toString: String = s"'$functionName($nestedFunctionGroup3, old_format = '$inputFormat', new_format = '$outputFormat')'"

  override protected val transformationFunction: Column => Column = date_format(_, inputFormat)
}
