package it.carloni.luca.aurora.spark.functions

import it.carloni.luca.aurora.spark.exception.UnmatchedFunctionException
import it.carloni.luca.aurora.utils.Utils.fullyMatchColOrLit
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

import scala.util.matching.Regex

abstract class ETLFunction(functionToApply: String, signature: Regex) {

  protected final val logger: Logger = Logger.getLogger(getClass)
  protected final val signatureMatch: Regex.Match = signature.findFirstMatchIn(functionToApply).get
  protected final val functionName: String = signatureMatch.group(1)
  val nestedFunctionGroup3: String = signatureMatch.group(3)

  protected val transformationFunction: Column => Column

  protected def toStringRepr: String

  def hasNestedFunction: Boolean = {

    !(nestedFunctionGroup3.equalsIgnoreCase("@") ||
      fullyMatchColOrLit(nestedFunctionGroup3))
  }

  def transform(inputColumn: Column): Column = {

    logger.info(s"Defined transformation: $toStringRepr")
    transformationFunction(inputColumn)
  }

  //noinspection SameParameterValue
  protected final def getColumnDefinitionAtGroup(n: Int): Column = {

    val nthArgument: String = signatureMatch.group(n)
    val startingColumn: Column = Signature.colOrLit.regex.findFirstMatchIn(nthArgument) match {

      case None => throw new UnmatchedFunctionException(nthArgument)
      case Some(x) =>

        val functionName: String = x.group(1)
        val functionArgument: String = x.group(2)
        val t3: (String => Column, String) = if (functionName equalsIgnoreCase "lit") (lit, "literal") else (col, "dataframe")

        val columnOp: String => Column = t3._1
        val columnType: String = t3._2

        logger.info(s"Identified $columnType column => $functionName('$functionArgument')")
        columnOp(functionArgument)
    }

    // IF THERE'S SOMETHING MORE THAN JUST A COL OR LIT ...
    if (!fullyMatchColOrLit(nthArgument)) {

      val suffix: String = n match {

        case 1 => "st"
        case 2 => "nd"
        case 3 => "rd"
        case _ => "th"

      }

      logger.info(s"Identified a transformation function for $n$suffix argument '$nthArgument'. Trying to resolve it")
      ETLFunctionFactory(nthArgument, startingColumn)

    } else startingColumn
  }
}
