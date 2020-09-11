package it.carloni.luca.aurora.spark.functions

import it.carloni.luca.aurora.spark.exception.UnmatchedFunctionException
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object ETLFunctionFactory {

  private final val logger: Logger = Logger.getLogger(getClass)

  def apply(functionToApply: String, inputColumn: Column): Column = {

    val matchingSignatures: Signature.ValueSet = Signature.values
      .filterNot(_ == Signature.dfColOrLit)
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      // RETRIEVE IT
      val matchedFunction: ETLFunction = matchingSignatures.head match {

        case Signature.dateFormat => DateFormatFunction(functionToApply)
        case Signature.leftOrRightPad => LeftOrRightPadFunction(functionToApply)
        case Signature.leftOrRightConcat => LeftOfRightConcatFunction(functionToApply)
        case Signature.leftOrRightConcatWs => LeftOrRightConcatWsFunction(functionToApply)
        case Signature.toDateOrTimestamp => ToDateOrTimestampFunction(functionToApply)
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
