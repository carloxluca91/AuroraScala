package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe._

case class FromDfTo[O](override protected val input: DataFrame,
                       override protected val outputKey: String,
                       private val dfToO: DataFrame => O)(implicit oTypeTag: TypeTag[O])
  extends IOStep[DataFrame, O](input, outputKey, s"FROM_DF_TO_${typeOf[O].getClass.getSimpleName}")
    with Logging {

  override protected def stepFunction(input: DataFrame): O = {

    val output: O = dfToO(input)
    log.info(s"Retrieved value: $output (class: ${output.getClass.getSimpleName})")
    output
  }
}
