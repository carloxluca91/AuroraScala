package it.luca.aurora.spark.sql.parser

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.sql.common._
import it.luca.aurora.spark.sql.functions._
import net.sf.jsqlparser.expression._
import net.sf.jsqlparser.expression.operators.relational.{ExpressionList, InExpression, IsNullExpression}
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.schema
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit, when}

import scala.collection.JavaConversions._

object SqlParser extends Logging {

  def parse(string: String): Column = {

    CCJSqlParserUtil.parseCondExpression(string, false) match {

      case column: schema.Column => col(column.getColumnName)
      case string: StringValue => lit(string.getValue)
      case long: LongValue => lit(long.getValue)
      case parenthesis: Parenthesis => parse(parenthesis.getExpression)
      case caze: CaseExpression => parseCaseExpression(caze)
      case binaryExpression: BinaryExpression => parseSqlBinaryExpression(binaryExpression)
      case isNullExpression: IsNullExpression => parseIsNullExpression(isNullExpression)
      case inExpression: InExpression => parseInExpression(inExpression)
      case function: Function => parseSqlFunction(function)
    }
  }

  def parse(expression: Expression): Column = parse(expression.toString)

  def parseSqlFunction(function: Function): Column = {

    // Standard Sql functions
    val sqlFunction: SqlFunction = function.getName.toLowerCase match {

      case "concat" => Concat(function)
      case "concat_ws" => ConcatWs(function)
      case "lpad" | "rpad" => LeftOrRightPad(function)
      case "substring" => Substring(function)
      case "to_date" | "to_timestamp" => ToDateOrTimestamp(function)
    }

    sqlFunction match {
      case ocf: OneColumnFunction =>

        val inputColumn = parse(function.getParameters.getExpressions.get(0))
        log.info(s"Parsed input column for ${ocf.getClass.getSimpleName} function")
        ocf.getColumn(inputColumn)

      case mcf: MultipleColumnFunction =>

        // Input parameters corresponding to input columns
        val inputColumnExpressions: java.util.List[Expression] = mcf match {

          // ConcatWs: exclude first parameter (which is separator)
          case _: ConcatWs => function.getParameters.getExpressions.tail
          case _ => function.getParameters.getExpressions
        }

        val inputColumns: Seq[Column] = inputColumnExpressions.map(parse)
        log.info(s"Parsed all of ${inputColumnExpressions.size()} input column(s) for ${mcf.getClass.getSimpleName} function")
        mcf.getColumn(inputColumns: _*)
    }
  }

  def parseSqlBinaryExpression(binaryExpression: BinaryExpression): Column = {

    // BinaryExpressions (<, <=, =, <>, >, >=, AND, OR)
    val leftColumn = parse(binaryExpression.getLeftExpression)
    val rightColumn = parse(binaryExpression.getRightExpression)
    log.info(s"Parsed both left and right expression of ${classOf[BinaryExpression].getSimpleName} $binaryExpression")
    val combinator: (Column, Column) => Column = binaryExpression.getStringExpression.toLowerCase match {
      case "<" => _ < _
      case "<=" => _ <= _
      case "=" => _ === _
      case "<>" => _ !== _
      case ">=" => _ >= _
      case ">" => _ > _
      case "and" => _ && _
      case "or" => _ || _
    }

    combinator(leftColumn, rightColumn)
  }

  def parseIsNullExpression(isNullExpression: IsNullExpression): Column = {

    val leftColumn = parse(isNullExpression.getLeftExpression)
    log.info(s"Parsed left expression for ${classOf[IsNullExpression].getSimpleName}")
    if (isNullExpression.isNot) leftColumn.isNotNull else leftColumn.isNull
  }

  def parseInExpression(inExpression: InExpression): Column = {

    val leftColumn = parse(inExpression.getLeftExpression)
    val inValuesColumns: Seq[Column] = inExpression
      .getRightItemsList(classOf[ExpressionList])
      .getExpressions
      .map(parse)

    log.info(s"Parsed both left and all of ${inValuesColumns.size()} right expression(s) of ${classOf[InExpression].getSimpleName}")
    val isInColumn = leftColumn.isin(inValuesColumns: _*)
    if (inExpression.isNot) !isInColumn else isInColumn
  }

  def parseCaseExpression(caze: CaseExpression): Column = {

    val whenCases: Seq[(Column, Column)] = caze.getWhenClauses.map(x => (parse(x.getWhenExpression), parse(x.getThenExpression)))
    val elseValue: Column = parse(caze.getElseExpression)
    log.info(s"Parsed both all of ${caze.getWhenClauses.size()} ${classOf[WhenClause].getSimpleName}(s) and ElseExpression")
    val firstCase: Column = when(whenCases.head._1, whenCases.head._2)
    whenCases.tail
      .foldLeft(firstCase)((col, tuple2) => col.when(tuple2._1, tuple2._2))
      .otherwise(elseValue)
  }
}
