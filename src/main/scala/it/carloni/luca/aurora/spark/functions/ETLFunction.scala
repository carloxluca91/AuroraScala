package it.carloni.luca.aurora.spark.functions

import it.carloni.luca.aurora.spark.exception.UnmatchedFunctionException
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

  def hasNestedFunction: Boolean = {

    (!nestedFunctionGroup3.equalsIgnoreCase("@")) ||
      Signature.colOrLit.regex.pattern.matcher(nestedFunctionGroup3).matches
  }

  def transform(inputColumn: Column): Column = transformationFunction(inputColumn)

  protected final def getColumnDefinitionAtGroup(id: Int): Column = {

    val nthFunctionArgument: String = signatureMatch.group(id)
    val startingColumn: Column = Signature.colOrLit.regex.findFirstMatchIn(nthFunctionArgument) match {

      case None => throw new UnmatchedFunctionException(nthFunctionArgument)
      case Some(x) =>

        val functioName: String = x.group(1)
        val functionArgument: String = x.group(2)

        logger.info(s"Indentified column '$functioName('$functionArgument')'")
        if (functioName equalsIgnoreCase "lit") lit(functionArgument)
        else col(functioName)
    }

    val matchesATransformationFunction: Boolean = Signature.values
      .filterNot(_ == Signature.colOrLit)
      .exists(_.regex.findFirstMatchIn(nthFunctionArgument).nonEmpty)

    if (matchesATransformationFunction) {

      val idStr: String = id match {
        case 1 => "1st"
        case 2 => "2nd"
        case 3 => "3rd"
        case other => s"${other}th"
      }

      logger.info(s"Identified a transformation function for $idStr argument. Trying to resolve it")
      ETLFunctionFactory(nthFunctionArgument, startingColumn)

    } else startingColumn
  }
}
