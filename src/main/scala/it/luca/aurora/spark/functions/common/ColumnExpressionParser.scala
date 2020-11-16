package it.luca.aurora.spark.functions.common

import it.luca.aurora.spark.exception.UnmatchedFunctionException
import it.luca.aurora.spark.functions.catalog.{ColExpression, LitExpression}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object ColumnExpressionParser {

  private final val logger = Logger.getLogger(getClass)

  def apply(stringExpression: String): Column = {

    val matchingExpressions: Seq[ColumnExpression.Value] = ColumnExpression.values
      .filter(_.regex.findFirstMatchIn(stringExpression).nonEmpty)
      .toSeq

    if (matchingExpressions.nonEmpty) {

      val matchingExpression: AbstractExpression = matchingExpressions.head match {

        case ColumnExpression.Col => ColExpression(stringExpression)
        case ColumnExpression.Lit => LitExpression(stringExpression)
      }

      val matchingExpressionAsString: String = matchingExpression.asString
      matchingExpression match {

        case expression: MultipleColumnExpression =>

          val subExpressionJoinStr = expression.subExpressions
            .map(x => s"'$x'")
            .mkString(", ")

          logger.info(s"Detected a multi column expression: $matchingExpressionAsString with following sub expressions ($subExpressionJoinStr). " +
            s"Trying to resolve each one recursively")

          val inputColumns: Seq[Column] = expression.subExpressions
            .map(ColumnExpressionParser(_))

          logger.info(s"Successfully resolved each sub expression")
          expression.getColumn(inputColumns: _*)

        case expression: SingleColumnExpression =>

          val nestedFunctionStr: String = expression.nestedFunction
          logger.info(s"Detected a static column expression: $matchingExpressionAsString with nested function '$nestedFunctionStr'. " +
            s"Trying to resolve this latter it recursively")
          expression.getColumn(ColumnExpressionParser(nestedFunctionStr))

        case expression: StaticColumnExpression =>

          logger.info(s"Detected a static column expression: $matchingExpressionAsString")
          expression.getColumn
      }
    } else {

      throw UnmatchedFunctionException(stringExpression)

    }
  }
}
