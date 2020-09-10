package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestampFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.toDateOrTimestamp.regex) {

  private final val inputFormat: String = signatureMatch.group(4)
  logger.info(toString)

  override def toString: String = s"'$functionName($nestedFunctionGroup3, format = '$inputFormat')'"

  override protected val transformationFunction: Column => Column = {

    if (functionName.equalsIgnoreCase("to_date")) to_date(_, inputFormat)
    else to_timestamp(_, inputFormat)
  }
}