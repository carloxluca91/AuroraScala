package it.luca.aurora.spark.step

import it.luca.aurora.core.Logging
import it.luca.aurora.core.utils.{now, toDate}
import it.luca.aurora.enumeration.{ColumnName, DateFormat}
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.collection.mutable

case class UpdateDfVersion(private val inputDfKey: String,
                           private val inputVersionKey: String,
                           override val outputKey: String)
  extends IOStep[DataFrame, DataFrame](s"Update ${ColumnName.Version} column on a DataFrame", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, DataFrame) = {

    val inputDf = variables(inputDfKey).asInstanceOf[DataFrame]
    val oldVersion = variables(inputVersionKey).asInstanceOf[String]
    val updatedVersion = f"${oldVersion.toDouble + 0.1}%.1f".replace(",", ".")
    log.info(s"Updating version from $oldVersion to $updatedVersion")
    val outputDf = inputDf.withColumn(ColumnName.ValidityStartTime, lit(now()))
      .withColumn(ColumnName.ValidityStartDate, lit(toDate(now(), DateFormat.DateDefault)))
      .withTechnicalColumns()
      .withColumn(ColumnName.Version, lit(updatedVersion))
      .withSqlNamingConvention()
      .coalesce(1)

    (outputKey, outputDf)
  }
}
