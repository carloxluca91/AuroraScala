package it.luca.aurora.spark

import org.apache.spark.sql.{DataFrame, DataFrameWriter, SQLContext}

package object implicits {

  implicit def extendDf(df: DataFrame): DataFrameExtended = new DataFrameExtended(df)

  implicit def extendDfWriter(dfWriter: DataFrameWriter): DataFrameWriterExtended = new DataFrameWriterExtended(dfWriter)

  implicit def extendSqlContext(sqlContext: SQLContext): SqlContextExtended = new SqlContextExtended(sqlContext)

}
