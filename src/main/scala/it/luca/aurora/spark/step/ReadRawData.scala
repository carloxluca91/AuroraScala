package it.luca.aurora.spark.step

import com.databricks.spark.avro._
import it.luca.aurora.Logging
import it.luca.aurora.enumeration.ColumnName
import it.luca.aurora.spark.bean.MappingRow
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable

case class ReadRawData(private val mappingRowKey: String,
                       private val dtBusinessDate: String,
                       private val sqlContext: SQLContext,
                       override val outputKey: String)
  extends IOStep[String, DataFrame](s"Read HDFS raw data files", outputKey) with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val mappingRow = variables(mappingRowKey).asInstanceOf[MappingRow]
    val inputHDFSPath = s"${mappingRow.inputPath}/${ColumnName.DtBusinessDate}=$dtBusinessDate"
    val rawDataDf: DataFrame = mappingRow.inputFileFormat.toLowerCase match {
      case "avro" => sqlContext.read.avro(inputHDFSPath)
      case "parquet" => sqlContext.read.parquet(inputHDFSPath)
    }

    log.info(
      s"""Loaded data from HDFS path $inputHDFSPath. Schema
         |
         |${rawDataDf.schema.treeString}
         |""".stripMargin)

    (outputKey, rawDataDf)
  }
}
