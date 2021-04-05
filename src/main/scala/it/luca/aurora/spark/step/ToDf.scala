package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import it.luca.aurora.utils.classSimpleName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class ToDf[T <: Product](private val beansKey: String,
                              private val sqlContext: SQLContext,
                              override val outputKey: String)(implicit classTag: ClassTag[T], typeTag: TypeTag[T])
  extends IOStep[Seq[T], DataFrame](s"Convert a Seq of ${classSimpleName[T]}(s) to a DataFrame", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val tClassName = classSimpleName[T]
    val beans = variables(beansKey).asInstanceOf[Seq[T]]
    log.info(s"Converting ${beans.size} $tClassName(s) to ${classSimpleName[DataFrame]}")
    val rddOfT: RDD[T] = sqlContext.sparkContext.parallelize(beans, 1)
    val dataFrame: DataFrame = sqlContext.createDataFrame(rddOfT).withSqlNamingConvention()
    log.info(s"""Converted ${beans.size} $tClassName(s) to ${classOf[DataFrame].getSimpleName}. Schema
                |
                |${dataFrame.schema.treeString}
                |""".stripMargin)
    (outputKey, dataFrame)
  }
}
