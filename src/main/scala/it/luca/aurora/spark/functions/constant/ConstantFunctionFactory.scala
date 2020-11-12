package it.luca.aurora.spark.functions.constant

import it.luca.aurora.spark.exception.UnmatchedFunctionException
import org.apache.spark.sql.Column

object ConstantFunctionFactory {

  def apply(functionToApply: String): Column = {

    val matchingSignatures: ConstantSignatures.ValueSet = ConstantSignatures.values
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      // RETRIEVE IT
      val matchedFunction: ConstantFunction = matchingSignatures.head match {

        case ConstantSignatures.nowAsDateOrTimestamp => NowAsDateOrTimestamp(functionToApply)
      }

      matchedFunction.getColumn

    } else {

      throw new UnmatchedFunctionException(functionToApply)
    }
  }
}
