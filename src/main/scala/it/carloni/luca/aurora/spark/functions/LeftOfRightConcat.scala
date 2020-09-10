package it.carloni.luca.aurora.spark.functions
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class LeftOfRightConcat(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.leftOrRightConcat.regex) {

  private final val otherConcatColumn: Column = getColumnDefinitionAtGroup(4)

  logger.info(toString)

  override def toString: String = {

    val firstArgument: String = if (functionName equalsIgnoreCase "lconcat") otherConcatColumn.toString else nestedFunctionGroup3
    val secondArgument: String = if (functionName equalsIgnoreCase "rconcat") otherConcatColumn.toString else nestedFunctionGroup3

    s"$functionName($nestedFunctionGroup3, $firstArgument, $secondArgument)"
  }

  override protected val transformationFunction: Column => Column = {

    if (functionName equalsIgnoreCase "lconcat") concat(otherConcatColumn, _)
    else concat(_, otherConcatColumn)
  }
}
