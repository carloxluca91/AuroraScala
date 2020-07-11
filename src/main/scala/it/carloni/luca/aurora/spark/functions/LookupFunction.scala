package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col, lower, trim, when}

class LookupFunction (inputColumn: Column, functionToApply: String, lookUpDataFrame: DataFrame)
  extends ETLFunction(inputColumn, functionToApply, Signatures.toTimestamp.regex) {

  private final val logger: Logger = Logger.getLogger(getClass)

  override def transform: Column = {

    val bancllName: String = signatureMatch.group(4)
    val rawColumnName: String = signatureMatch.group(5)

    logger.info(s"Function: '$functionName', BANCLL name: '$bancllName', raw column name: '$rawColumnName'")
    val lookUpCases: Array[Row]  = lookUpDataFrame
      .filter((trimAndLower(col("bancll_name")) === bancllName.toLowerCase) &&
        (trimAndLower(col("column_name")) === rawColumnName.toLowerCase))
      .selectExpr("original_value", "replacement_value")
      .collect()

    //TODO: check if lookUpCases not empty
    if (lookUpCases.isEmpty) {

     logger.error(s"Unable to retrieve any case pair for BANCLL '$bancllName', raw column name: '$rawColumnName'")
     throw new Exception

    } else {

      logger.info(s"Retrieved ${lookUpCases.length} case pairs related to BANCLL '$bancllName', raw column name: '$rawColumnName'")

      val lookUpCasesAsStr: String = lookUpCases
        .map(row => s"'${row.get(0)}' -> '${row.get(1)}'")
        .mkString(", ")

      logger.info(s"Defining following replacing rules: $lookUpCasesAsStr")
      val lookUpHeadCase: (Any, Any) = (lookUpCases.head.get(0), lookUpCases.head.get(1))
      val lookUpHeadCaseCol: Column = when(inputColumn === lookUpHeadCase._1, lookUpHeadCase._2)
      lookUpCases
        .tail
        .foldLeft(lookUpHeadCaseCol)((c: Column, row: Row) => {
          c.when(inputColumn === row.get(0), row.get(1))})
    }
  }

  private def trimAndLower(column: Column): Column = lower(trim(column))
}
