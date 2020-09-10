package it.carloni.luca.aurora.spark.functions
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class LeftOfRightConcat(functionToApply: String)
  extends ETLFunction(functionToApply, Signature.leftOrRightConcat.regex) {

  private final val otherConcatColumn: Column = getColumnDefinitionAtGroup(4)

  override protected val transformationFunction: Column => Column = {

    if (functionName equalsIgnoreCase "lconcat") concat(otherConcatColumn, _)
    else concat(_, otherConcatColumn)
  }
}
