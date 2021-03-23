package it.luca.aurora.spark.sql.functions.common

import it.luca.aurora.exception.UnmatchedFunctionException
import it.luca.aurora.spark.sql.functions.catalog._
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object ColumnExpressionParser {

  private final val logger = Logger.getLogger(getClass)

  def apply(stringExpression: String): Column = {

    // Detect the regex matching the provided expression
    val matchingExpressions: Seq[ColumnExpression.Value] = ColumnExpression.values
      .filter(_.regex.findFirstMatchIn(stringExpression).nonEmpty)
      .toSeq

    // If one has been found
    if (matchingExpressions.nonEmpty) {

      val matchingExpression: AbstractExpression = matchingExpressions.head match {

        case ColumnExpression.Cast => Cast(stringExpression)
        case ColumnExpression.Col => Col(stringExpression)
        case ColumnExpression.LeftOrRightPad => LeftOrRightPad(stringExpression)
        case ColumnExpression.Lit => Lit(stringExpression)
        case ColumnExpression.LowerOrUpper => LowerOrUpper(stringExpression)
        case ColumnExpression.OrElse => OrElse(stringExpression)
        case ColumnExpression.Replace => Replace(stringExpression)
        case ColumnExpression.Substring => Substring(stringExpression)
        case ColumnExpression.ToDateOrTimestamp => ToDateOrTimestamp(stringExpression)
        case ColumnExpression.Trim => Trim(stringExpression)
      }

      val matchingExpressionAsString: String = matchingExpression.asString
      matchingExpression match {

        // Static column
        case expression: StaticColumnExpression =>

          logger.info(s"Detected a static column expression: $matchingExpressionAsString")
          expression.getColumn

        // Single column expression (i.e. standard ETL function)
        case expression: SingleColumnExpression =>

          val nestedFunctionStr: String = expression.nestedFunction
          logger.info(s"Detected a single column expression: $matchingExpressionAsString with nested function '$nestedFunctionStr'. " +
            s"Trying to resolve this latter recursively")
          expression.getColumn(ColumnExpressionParser(nestedFunctionStr))

        // Multiple column expression (i.e. ETL function accepting many columns)
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
      }
    } else throw UnmatchedFunctionException(stringExpression)
  }
}
