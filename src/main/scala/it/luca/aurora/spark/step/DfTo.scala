package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.classSimpleName
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.reflect.runtime.universe._

case class DfTo[O](private val dfKey: String,
                   private val dfToO: DataFrame => O,
                   override val outputKey: String)(implicit oTypeTag: TypeTag[O])
  extends IOStep[DataFrame, O]( s"Retrieves a ${classSimpleName[O]} from a DataFrame", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, O) = {

    val inputDf: DataFrame = variables(dfKey).asInstanceOf[DataFrame]
    val output = dfToO(inputDf)
    log.info(s"Retrieved value: $output (class: ${output.getClass.getSimpleName})")
    (outputKey, output)
  }
}