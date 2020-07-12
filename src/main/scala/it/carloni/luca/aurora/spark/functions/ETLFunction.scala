package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ETLFunction(functionToApply: String, signature: Regex) {

  private final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)

  logger.info(s"Identified function '$functionName'")

  private final val nestedFunctionGroup2Opt: Option[String] = Option(signatureMatch.group(2))
  private final val nestedFunctionGroup3Opt: Option[String] = Option(signatureMatch.group(3))

  protected def getColumnToTransform(inputColumn: Column): Column = {

    if (nestedFunctionGroup2Opt.isEmpty) {

      logger.info("No nested function identified. Transforming given input column")
      inputColumn

    } else {

      val group2Value: String = nestedFunctionGroup2Opt.get
      logger.info(s"Identified string related to a nested function: '$group2Value'")

      val nestedFunctionStr: String = nestedFunctionGroup3Opt.getOrElse(group2Value)
      logger.info(s"Nested function 'clean' definition: '$nestedFunctionStr'")
      ETLFunctionFactory(nestedFunctionStr).transform(inputColumn)
    }
  }

  def transform(inputColumn: Column): Column
}
