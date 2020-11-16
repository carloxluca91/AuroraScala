package it.luca.aurora.spark.functions.catalog

import it.luca.aurora.spark.functions.common.StaticColumnExpression
import it.luca.aurora.spark.functions.common.ColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class ColExpression(override val string: String)
  extends StaticColumnExpression(string, ColumnExpression.Col) {

  final val columnName: String = group(2)

  override def asString: String = s"${functionName.toUpperCase}('$columnName')"

  override def getColumn: Column = col(columnName)
}
