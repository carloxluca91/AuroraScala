package it.carloni.luca.aurora.spark.functions
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class LeftOfRightConcatFunction(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.leftOrRightConcat.regex) {

  private final val concatColumn: Column = getColumnDefinitionAtGroup(4)

  override protected val transformationFunction: Column => Column = {

    if (functionName startsWith  "l") concat(concatColumn, _)
    else concat(_, concatColumn)
  }

  override protected def toStringRepr: String = {

    val concatSequenceQuoted: String = s"concat_column = '${concatColumn.toString()}'"
    val firstArgument: String = if (functionName startsWith  "l") concatSequenceQuoted else nestedFunctionGroup3
    val secondArgument: String = if (functionName startsWith  "r") concatSequenceQuoted else nestedFunctionGroup3

    s"concat($firstArgument, $secondArgument)"
  }
}
