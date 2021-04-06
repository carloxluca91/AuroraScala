package it.luca.aurora.spark.step

import it.luca.aurora.Logging
import it.luca.aurora.spark.bean.SpecificationRows
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformDfWrtSpecifications(private val inputDfKey: String,
                                        private val specificationInputKey: String,
                                        private val dfTransformation: (DataFrame, SpecificationRows) => DataFrame,
                                        override val outputKey: String)
  extends IOStep[(DataFrame, SpecificationRows), DataFrame]("Transform DataFrame according to specifications", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val inputDf = variables(inputDfKey).asInstanceOf[DataFrame]
    val specificationRows = variables(specificationInputKey).asInstanceOf[SpecificationRows]
    log.info(
      s"""Input dataframe schema
         |
         |  ${inputDf.schema.treeString}
         |  """.stripMargin)

    val outputDf = dfTransformation(inputDf, specificationRows)
    log.info(
      s"""Output dataframe schema
         |
         |    ${outputDf.schema.treeString}
         |""".stripMargin)
    (outputKey, outputDf)
  }
}
