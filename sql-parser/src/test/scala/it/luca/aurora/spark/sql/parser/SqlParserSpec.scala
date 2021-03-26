package it.luca.aurora.spark.sql.parser

import it.luca.aurora.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.scalatest

class SqlParserSpec extends BaseSpec {

  private def assertExpressionEqualsColumn(string: String, expectedColumn: Column): scalatest.Assertion = {

    assertResult(expectedColumn) {
      SqlParser.parse(string)
    }
  }

  s"A ${SqlParser.getClass.getSimpleName}" must
    s"be able to parse ${clazz[Concat]} function" in {

    val string = s"concat($firstColumnName, $secondColumnName)"
    val expectedColumn = concat(firstColumn, secondColumn)
    assertExpressionEqualsColumn(string, expectedColumn)
  }

  it must s"be able to parse ${clazz[ConcatWs]} function" in {

    val separator = "-"
    val string = s"concat_ws('$separator', $firstColumnName, $secondColumnName)"
    val expectedColumn = concat_ws(separator, firstColumn, secondColumn)
    assertExpressionEqualsColumn(string, expectedColumn)
  }

  it must s"be able to parse ${clazz[LeftOrRightPad]} function" in {

    val paddingLength = 5
    val paddingString = "0"
    val string = s"lpad($firstColumnName, $paddingLength, '$paddingString')"
    val expectedColumn = lpad(firstColumn, paddingLength, paddingString)
    assertExpressionEqualsColumn(string, expectedColumn)
    assertExpressionEqualsColumn(string.replaceFirst("l", "r"), rpad(firstColumn, paddingLength, paddingString))
  }

  it must s"be able to parse ${clazz[Substring]} function" in {

    val start = 0
    val end = 2
    val string = s"substring($firstColumnName, $start, $end)"
    val expectedColumn = substring(firstColumn, start, end)
    assertExpressionEqualsColumn(string, expectedColumn)
  }

  it must s"be able to parse ${clazz[ToDateOrTimestamp]} function" in {

    val pattern = "yyyy-MM-dd"
    val string = s"to_timestamp($firstColumnName, '$pattern')"
    val expectedColumn = from_unixtime(unix_timestamp(firstColumn, pattern))
    assertExpressionEqualsColumn(string, expectedColumn)
    assertExpressionEqualsColumn(string.replaceFirst("timestamp", "date"), to_date(expectedColumn))
  }
}
