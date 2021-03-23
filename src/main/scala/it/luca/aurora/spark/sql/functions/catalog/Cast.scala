package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.{ColumnExpression, SingleColumnExpression}
import it.luca.aurora.utils.Utils
import org.apache.spark.sql.Column

case class Cast(override val stringExpression: String)
  extends SingleColumnExpression(stringExpression, ColumnExpression.Cast) {

  final val dataType: String = group(3)

  override def asString: String = s"${functionName.toUpperCase}($nestedFunction, DATA_TYPE = '$dataType')"

  override def getColumn(inputColumn: Column): Column = inputColumn.cast(Utils.resolveDataType(dataType))

}
