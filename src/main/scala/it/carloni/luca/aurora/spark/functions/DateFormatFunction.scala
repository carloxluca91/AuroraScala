package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}

case class DateFormatFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signatures.dateFormat.regex) {

  private final val logger: Logger = Logger.getLogger(getClass)
  private final val inputFormat: String = signatureMatch.group(4)
  private final val outputFormat: String = signatureMatch.group(5)

  override def transform(inputColumn: Column): Column = {

    logger.info(s"Function: '$functionName', Input format: '$inputFormat', Output format: '$outputFormat'")
    from_unixtime(unix_timestamp(getColumnToTransform(inputColumn), inputFormat), outputFormat)
  }
}
