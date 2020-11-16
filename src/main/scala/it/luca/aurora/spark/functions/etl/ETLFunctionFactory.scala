package it.luca.aurora.spark.functions.etl

import it.luca.aurora.spark.exception.UnmatchedFunctionException
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object ETLFunctionFactory {

  private final val logger: Logger = Logger.getLogger(getClass)

  def apply(functionToApply: String, inputColumn: Column): Column = {

    val matchingSignatures: ColumnExpression.ValueSet = ColumnExpression.values
      .filterNot(_ == ColumnExpression.Col)
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      // RETRIEVE IT
      val matchedFunction: ETLFunction = matchingSignatures.head match {

        case ColumnExpression.dateFormat => DateFormatFunction(functionToApply)
        case ColumnExpression.leftOrRightPad => LeftOrRightPadFunction(functionToApply)
        case ColumnExpression.leftOrRightConcat => LeftOfRightConcatFunction(functionToApply)
        case ColumnExpression.leftOrRightConcatWs => LeftOrRightConcatWsFunction(functionToApply)
        case ColumnExpression.toDateOrTimestamp => ToDateOrTimestampFunction(functionToApply)
        case ColumnExpression.toDateY2 => ToDateY2(functionToApply)
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
