package it.luca.aurora.spark.step

import it.luca.aurora.Logging
import it.luca.aurora.utils.className
import it.luca.aurora.excel.implicits._
import it.luca.aurora.spark.implicits._
import org.apache.poi.ss.usermodel.{Row, Workbook}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class SheetToDf[T <: Product](private val workbookKey: String,
                                   private val sheetIndex: Int,
                                   private val sqlContext: SQLContext,
                                   override val outputKey: String)
                                  (implicit classTag: ClassTag[T], typeTag: TypeTag[T], val decodeRow: Row => T)
  extends IOStep[Workbook, DataFrame](s"Decode Excel sheet # $sheetIndex and turn it into a ${className[DataFrame]}", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val tClassName = className[T]
    val workbook = variables(workbookKey).asInstanceOf[Workbook]
    val beans: Seq[T] = workbook.as[T](sheetIndex)
    log.info(s"Decoded sheet # $sheetIndex as ${beans.size} $tClassName(s). Converting them to a ${className[DataFrame]}")
    val rddOfT: RDD[T] = sqlContext.sparkContext.parallelize(beans, 1)
    val dataFrame: DataFrame = sqlContext.createDataFrame(rddOfT).withSqlNamingConvention()
    log.info(s"""Converted ${beans.size} $tClassName(s) to ${className[DataFrame]}. Schema
                |
                |${dataFrame.schema.treeString}
                |""".stripMargin)
    (outputKey, dataFrame)
  }
}
