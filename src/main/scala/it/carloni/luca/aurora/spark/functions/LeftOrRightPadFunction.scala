package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lpad, rpad}

case class LeftOrRightPadFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signatures.leftOrRightPad.regex) {

  private final val paddingLength: Int = signatureMatch.group(4).toInt
  private final val paddingString: String = signatureMatch.group(5)

  override def toString: String = s"Function: '$functionName', length to pad: '$paddingLength', padding charsequence: '$paddingString'"

  override def transform(inputColumn: Column): Column =

    if (functionName.toLowerCase.startsWith("l")) lpad(inputColumn, paddingLength, paddingString)
    else rpad(inputColumn, paddingLength, paddingString)
}
