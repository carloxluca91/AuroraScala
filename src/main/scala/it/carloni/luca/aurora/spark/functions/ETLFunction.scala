package it.carloni.luca.aurora.spark.functions

import it.carloni.luca.aurora.spark.engine.RawDataTransformerEngine
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ETLFunction(val inputColumn: Column, val functionToApply: String, val signature: Regex) {

  private final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)

  logger.info(s"Identified function '$functionName'")

  protected final val nestedFunctionGroup2Opt: Option[String] = Option(signatureMatch.group(2))
  protected final val nestedFunctionGroup3Opt: Option[String] = Option(signatureMatch.group(3))
  protected final val nestedFunctionCol: Column = getNestedFunctionCol

  private def getNestedFunctionCol: Column = {

    nestedFunctionGroup2Opt match {

      case None =>

        logger.info("No further nested function has been found")
        inputColumn

      case Some(group2Value) =>

        logger.info(s"Identified string related to a nested function: '$group2Value'")
        val nestedFunctionValue: String = nestedFunctionGroup3Opt match {

          case None => group2Value
          case Some(group3Value) => group3Value
        }

        logger.info(s"Nested function clean definition: '$nestedFunctionValue'")
        RawDataTransformerEngine(inputColumn, nestedFunctionValue)
    }
  }

  def transform: Column
}
