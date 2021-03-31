package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.Utils.classSimpleName
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe._

case class DfTo[O](override val input: DataFrame,
                   override val outputKey: String,
                   private val dfToO: DataFrame => O)(implicit oTypeTag: TypeTag[O])
  extends IOStep[DataFrame, O](input, outputKey, s"FROM_DF_TO_${classSimpleName[O].toUpperCase}")
    with Logging {

  override protected def stepFunction(input: DataFrame): O = {

    val output: O = dfToO(input)
    log.info(s"Retrieved value: $output (class: ${output.getClass.getSimpleName})")
    output
  }
}
