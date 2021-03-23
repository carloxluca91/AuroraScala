package it.luca.aurora.spark.sql.functions

import it.luca.aurora.spark.sql.common.OneColumnFunction
import net.sf.jsqlparser.expression.{Function, StringValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{from_unixtime, to_date, unix_timestamp}

case class ToDateOrTimestamp(override protected val function: Function)
  extends OneColumnFunction(function) {

  override def getColumn(column: Column): Column = {

    val format: String = getFunctionParameter[StringValue, String](1, _.getValue)
    val timestampColumn: Column = from_unixtime(unix_timestamp(column, format))
    if (functionName.toLowerCase.endsWith("date")) to_date(timestampColumn) else timestampColumn
  }
}
