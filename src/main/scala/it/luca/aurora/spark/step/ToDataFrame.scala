package it.luca.aurora.spark.step

import it.luca.aurora.enumeration.JobVariable
import it.luca.aurora.spark.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

case class ToDataFrame[T <: Product](override protected val input: Seq[T],
                                     private val sqlContext: SQLContext,
                                     private val jobVariable: JobVariable.Value)
  extends IOStep[Seq[T], DataFrame](input, s"${classOf[T].getSimpleName}_TO_DATAFRAME") {

  override def run(): (JobVariable.Value, DataFrame) = {

    val rddOfT: RDD[T] = sqlContext.sparkContext.parallelize(input, 1)
    val tDf: DataFrame = sqlContext.createDataFrame(rddOfT)
    (jobVariable, tDf.withSqlNamingConvention())
  }
}
