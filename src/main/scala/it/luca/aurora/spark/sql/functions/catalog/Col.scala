package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.StaticColumnExpression
import it.luca.aurora.spark.sql.functions.common.ColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class Col(override val stringExpression: String)
  extends StaticColumnExpression(stringExpression, ColumnExpression.Col) {

  final val columnName: String = group(2)

  override def asString: String = s"${functionName.toUpperCase}('$columnName')"

  override def getColumn: Column = col(columnName)
}
