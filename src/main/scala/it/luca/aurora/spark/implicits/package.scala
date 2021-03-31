package it.luca.aurora.spark

import it.luca.aurora.enumeration.ColumnName
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, SQLContext}

package object implicits {

  implicit def columnNameToString(columnName: ColumnName.Value): String = columnName.name

  implicit def extendColumn(column: Column): ColumnExtended = new ColumnExtended(column)

  implicit def extendDf(df: DataFrame): DataFrameExtended = new DataFrameExtended(df)

  implicit def extendDfWriter(dfWriter: DataFrameWriter): DataFrameWriterExtended = new DataFrameWriterExtended(dfWriter)

  implicit def extendSqlContext(sqlContext: SQLContext): SqlContextExtended = new SqlContextExtended(sqlContext)
}
