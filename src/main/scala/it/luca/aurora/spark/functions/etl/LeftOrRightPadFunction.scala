package it.luca.aurora.spark.functions.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPadFunction(functionToApply: String)
  extends ETLFunction(functionToApply, ETLSignatures.leftOrRightPad.regex) {

  private final val paddingLength: Int = signatureMatch.group(4).toInt
  private final val paddingString: String = signatureMatch.group(5)

  override protected val transformationFunction: Column => Column = {

    if (functionName.toLowerCase.startsWith("l")) lpad(_, paddingLength, paddingString)
    else rpad(_, paddingLength, paddingString)
  }

  override protected def toStringRepr: String =

    s"$functionName($nestedFunctionGroup3, padding_length = $paddingLength, padding_charsequence = '$paddingString')"
}
