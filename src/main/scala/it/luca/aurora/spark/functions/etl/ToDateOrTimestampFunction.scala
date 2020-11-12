package it.luca.aurora.spark.functions.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_date, to_timestamp}

case class ToDateOrTimestampFunction(functionToApply: String)
  extends ETLFunction(functionToApply, ETLSignatures.toDateOrTimestamp.regex) {

  private final val inputFormat: String = signatureMatch.group(4)

  override protected val transformationFunction: Column => Column = {

    if (functionName.equalsIgnoreCase("to_date")) to_date(_, inputFormat)
    else to_timestamp(_, inputFormat)
  }

  override protected def toStringRepr: String = s"$functionName($nestedFunctionGroup3, format = '$inputFormat')"
}
