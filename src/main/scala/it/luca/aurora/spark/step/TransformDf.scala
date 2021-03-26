package it.luca.aurora.spark.step

import org.apache.spark.sql.DataFrame

case class TransformDf(override protected val input: DataFrame,
                       override protected val outputKey: String,
                       private val dfTransformation: DataFrame => DataFrame)
  extends IOStep[DataFrame, DataFrame](input,
    stepName =  s"TRANSFORM_DATAFRAME",
    outputKey = outputKey) {

  override protected def stepFunction(input: DataFrame): DataFrame = dfTransformation(input)
}
