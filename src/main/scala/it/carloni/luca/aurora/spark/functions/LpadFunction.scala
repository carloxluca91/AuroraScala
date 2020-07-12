package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lpad

case class LpadFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signatures.lpad.regex) {

  private final val logger: Logger = Logger.getLogger(getClass)

  override def transform(inputColumn: Column): Column = {

    val paddingLength: Int = signatureMatch.group(4).toInt
    val paddingString: String = signatureMatch.group(5)

    logger.info(s"Function: '$functionName', length to pad: '$paddingLength', padding charsequence: '$paddingString'")
    lpad(getColumnToTransform(inputColumn), paddingLength, paddingString)
  }
}
