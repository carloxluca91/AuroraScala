package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ETLFunction(val column: Column, val functionToApply: String, val signature: Regex) {

  protected final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)

  logger.info(s"Identified function $functionName")

  protected final val nestedFunctionOpt: Option[String] = Option(signatureMatch.group(2))
  protected final val nestedFunctionCol: Column = getNestedFunctionCol

  protected def getNestedFunctionCol: Column = {

    nestedFunctionOpt match {

      case None =>

        logger.info("No further nested function has been found")
        column

      case Some(value) =>

        logger.info(s"Nested function identified: $value")
        Factory(column, value)
    }
  }

  def transform: Column
}
