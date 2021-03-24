package it.luca.aurora.spark.implicits

import org.apache.spark.sql.DataFrameWriter

class DataFrameWriterExtended(private val dataFrameWriter: DataFrameWriter) {

  def partitionBy(opt: Option[Seq[String]]): DataFrameWriter = {

   opt.map(dataFrameWriter.partitionBy(_: _*)).getOrElse(dataFrameWriter)

  }
}
