package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformDf(private val inputDfKey: String,
                       private val dfTransformation: DataFrame => DataFrame,
                       override val outputKey: String)
  extends IOStep[DataFrame, DataFrame]("Transform DataFrame",outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val inputDf = variables(inputDfKey).asInstanceOf[DataFrame]
    log.info(
      s"""Input dataframe schema
         |
         |${inputDf.schema.treeString}
         |""".stripMargin)

    val outputDf = dfTransformation(inputDf)
    log.info(
      s"""Output dataframe schema
         |
         |${outputDf.schema.treeString}
         |""".stripMargin)
    (outputKey, outputDf)
  }
}
