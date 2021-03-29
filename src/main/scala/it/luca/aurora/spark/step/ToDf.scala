package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

case class ToDf[T <: Product](override protected val input: Seq[T],
                              override protected val outputKey: String,
                              private val sqlContext: SQLContext)
  extends IOStep[Seq[T], DataFrame](input,
    stepName = s"${classOf[T].getSimpleName.toUpperCase}_TO_DATAFRAME",
    outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: Seq[T]): DataFrame = {

    val tClassName = classOf[T].getSimpleName
    log.info(s"Converting ${input.size} $tClassName(s) to ${classOf[DataFrame].getSimpleName}")
    val rddOfT: RDD[T] = sqlContext.sparkContext.parallelize(input, 1)
    val dataFrame: DataFrame = sqlContext.createDataFrame(rddOfT).withSqlNamingConvention()
    log.info(s"""Converted ${input.size} $tClassName(s) to ${classOf[DataFrame].getSimpleName}. Schema
         |
         |    ${dataFrame.schema.treeString}
         |    """.stripMargin)

    dataFrame
  }
}
