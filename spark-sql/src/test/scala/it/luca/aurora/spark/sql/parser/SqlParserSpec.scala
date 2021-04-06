package it.luca.aurora.spark.sql.parser

import it.luca.aurora.CustomSpec
import it.luca.aurora.spark.sql.functions._
import net.sf.jsqlparser.expression.BinaryExpression
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.scalatest

class SqlParserSpec extends CustomSpec {

  private val firstColumnName = "first_col"
  private val secondColumnName = "second_col"
  private val firstColumn = col(firstColumnName)
  private val secondColumn = col(secondColumnName)

  private def areEquivalent(string: String, expectedColumn: Column): scalatest.Assertion = {

    assertResult(expectedColumn.toString()) {
      SqlParser.parse(string).toString()
    }
  }

  /*
  s"A ${SqlParser.getClass.getSimpleName}" must s"parse a ${clazz[CaseExpression]}" in {

    val (firstValue, secondValue, eleValue) = ("GT10", "GT2", "BOH")
    val string = s"CASE WHEN $firstColumnName > 10 THEN '$firstValue' WHEN $firstColumnName > 2 THEN '$secondValue' ELSE '$eleValue' END"
    val expected = when(firstColumn > 10, firstValue).when(firstColumn > 2, secondValue).otherwise(eleValue)
    areEquivalent(string, expected)
  }

   */

  it must s"parse a ${className[BinaryExpression]}" in {

    val seq: Map[String, Column] = Map("<" -> firstColumn.lt(secondColumn),
      "<=" -> firstColumn.leq(secondColumn),
      "=" -> firstColumn.equalTo(secondColumn),
      "<>" -> firstColumn.notEqual(secondColumn),
      ">=" -> firstColumn.geq(secondColumn),
      ">" -> firstColumn.gt(secondColumn))

    seq.foreach{ case (key, value) =>
      areEquivalent(s"$firstColumnName $key $secondColumnName", value)
    }
  }

  it must s"parse a ${className[IsNullExpression]}" in {

    areEquivalent(s"$firstColumnName IS NULL", firstColumn.isNull)
    areEquivalent(s"$firstColumnName IS NOT NULL", firstColumn.isNotNull)
  }

  it must s"parse ${className[Concat]} function" in {

    val string = s"concat($firstColumnName, $secondColumnName)"
    val expectedColumn = concat(firstColumn, secondColumn)
    areEquivalent(string, expectedColumn)
  }

  it must s"parse ${className[ConcatWs]} function" in {

    val separator = "-"
    val string = s"concat_ws('$separator', $firstColumnName, $secondColumnName)"
    val expectedColumn = concat_ws(separator, firstColumn, secondColumn)
    areEquivalent(string, expectedColumn)
  }

  it must s"parse ${className[LeftOrRightPad]} function" in {

    val (paddingLength, paddingString) = (5, "0")
    val string = s"lpad($firstColumnName, $paddingLength, '$paddingString')"
    val expectedColumn = lpad(firstColumn, paddingLength, paddingString)
    areEquivalent(string, expectedColumn)
    areEquivalent(string.replaceFirst("l", "r"), rpad(firstColumn, paddingLength, paddingString))
  }

  it must s"parse ${className[Substring]} function" in {

    val (start, end) = (0, 2)
    val string = s"substring($firstColumnName, $start, $end)"
    val expectedColumn = substring(firstColumn, start, end)
    areEquivalent(string, expectedColumn)
  }

  it must s"parse ${className[ToDateOrTimestamp]} function" in {

    val pattern = "yyyy-MM-dd"
    val string = s"to_timestamp($firstColumnName, '$pattern')"
    val expectedColumn = from_unixtime(unix_timestamp(firstColumn, pattern))
    areEquivalent(string, expectedColumn)
    areEquivalent(string.replaceFirst("timestamp", "date"), to_date(expectedColumn))
  }
}
