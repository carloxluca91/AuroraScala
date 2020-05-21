package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object FunctionFactory {

  private final val logger = Logger.getLogger(getClass)

  def apply(column: Column, functionToApply: String): Column = {

    val matchingSignatures: FunctionSignature.ValueSet = FunctionSignature.values
      .filter(_.signatureRegex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      val matchingSignature: FunctionSignature.Value = matchingSignatures.head
      matchingSignature match {

        case FunctionSignature.dateFormat => column
        case FunctionSignature.lpad => column
        case FunctionSignature.rpad => column
      }
    }

    else {

      // TODO: lancio eccezione personalizzata
      logger.error(s"Unable to match such function: $functionToApply")
      throw new Exception
    }
  }

}
