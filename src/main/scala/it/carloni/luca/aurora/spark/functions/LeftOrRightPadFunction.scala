package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPadFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.leftOrRightPad.regex) {

  private final val paddingLength: Int = signatureMatch.group(4).toInt
  private final val paddingString: String = signatureMatch.group(5)

  logger.info(toString)

  override def toString: String = s"'$functionName($nestedFunctionGroup3, padding_length = $paddingLength, padding charsequence = '$paddingString')'"

  override protected val transformationFunction: Column => Column = {

    if (functionName.toLowerCase.startsWith("l")) lpad(_, paddingLength, paddingString)
    else rpad(_, paddingLength, paddingString)
  }
}
