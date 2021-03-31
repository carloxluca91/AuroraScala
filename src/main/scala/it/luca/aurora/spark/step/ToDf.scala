package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import it.luca.aurora.utils.Utils.classSimpleName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class ToDf[T <: Product](override val input: Seq[T],
                              override val outputKey: String,
                              private val sqlContext: SQLContext)(implicit classTag: ClassTag[T], typeTag: TypeTag[T])
  extends IOStep[Seq[T], DataFrame](input, stepName = s"${classSimpleName[T].toUpperCase}_TO_DATAFRAME",
    outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: Seq[T]): DataFrame = {

    val tClassName = classSimpleName[T]
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
