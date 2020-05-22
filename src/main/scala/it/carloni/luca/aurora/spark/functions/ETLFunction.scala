package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.util.matching.Regex

abstract class ETLFunction(val column: Column, val functionToApply: String, val signature: Regex) {

  protected final val logger: Logger = Logger.getLogger(getClass)
  protected final val matcher: java.util.regex.Matcher = signature.pattern.matcher(functionToApply)
  protected final val functionName: String = matcher.group(1)
  protected final val nestedFunction: String = matcher.group(2)
  protected final val nestedFunctionCol: Column = getNestedFunctionCol

  protected def getNestedFunctionCol: Column = {

    Option(nestedFunction) match {

      case None =>

        logger.info("No nested function has been found")
        column

      case Some(value) =>

        logger.info(s"Nested function identified: $nestedFunction")
        Factory(column, value)
    }
  }

  def transform: Column
}
