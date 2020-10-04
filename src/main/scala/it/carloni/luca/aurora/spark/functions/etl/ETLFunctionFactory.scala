package it.carloni.luca.aurora.spark.functions.etl

import it.carloni.luca.aurora.spark.exception.UnmatchedFunctionException
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object ETLFunctionFactory {

  private final val logger: Logger = Logger.getLogger(getClass)

  def apply(functionToApply: String, inputColumn: Column): Column = {

    val matchingSignatures: ETLSignatures.ValueSet = ETLSignatures.values
      .filterNot(_ == ETLSignatures.dfColOrLit)
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      // RETRIEVE IT
      val matchedFunction: ETLFunction = matchingSignatures.head match {

        case ETLSignatures.dateFormat => DateFormatFunction(functionToApply)
        case ETLSignatures.leftOrRightPad => LeftOrRightPadFunction(functionToApply)
        case ETLSignatures.leftOrRightConcat => LeftOfRightConcatFunction(functionToApply)
        case ETLSignatures.leftOrRightConcatWs => LeftOrRightConcatWsFunction(functionToApply)
        case ETLSignatures.toDateOrTimestamp => ToDateOrTimestampFunction(functionToApply)
        case ETLSignatures.toDateY2 => ToDateY2(functionToApply)
      }

      val columnToTransform: Column = if (matchedFunction.hasNestedFunction) {

        logger.info(s"Detected nested function: '${matchedFunction.nestedFunctionGroup3}'. Trying to resolve it")
        ETLFunctionFactory(matchedFunction.nestedFunctionGroup3, inputColumn)

      } else inputColumn

      matchedFunction.transform(columnToTransform)

    } else {

      throw new UnmatchedFunctionException(functionToApply)
    }
  }
}
