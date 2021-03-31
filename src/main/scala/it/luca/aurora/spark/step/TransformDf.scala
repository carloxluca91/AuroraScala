package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.DataFrame

case class TransformDf(override val input: DataFrame,
                       override val outputKey: String,
                       private val dfTransformation: DataFrame => DataFrame)
  extends IOStep[DataFrame, DataFrame](input, stepName =  s"TRANSFORM_DATAFRAME", outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: DataFrame): DataFrame = {

    log.info(
      s"""Input dataframe schema
         |
         |  ${input.schema.treeString}
         |  """.stripMargin)

    val outputDf = dfTransformation(input)
    log.info(
      s"""Output dataframe schema
         |
         |    ${outputDf.schema.treeString}
         |""".stripMargin)
    outputDf
  }
}
