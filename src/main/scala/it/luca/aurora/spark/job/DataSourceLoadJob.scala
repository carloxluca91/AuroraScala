package it.luca.aurora.spark.job
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.option.DataSourceLoadConfig
import it.luca.aurora.spark.step.Step
import org.apache.spark.sql.SQLContext

case class DataSourceLoadJob(override protected val sqlContext: SQLContext,
                             override protected val propertiesFile: String,
                             private val config: DataSourceLoadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.SourceLoad) {

  override protected val dataSource: Option[String] = Some(config.dataSource)
  override protected val dtBusinessDate: Option[String] = config.dtBusinessDate
  override protected val specificationVersion: Option[String] = Some(config.specificationVersion)

  override protected val steps: Seq[Step[_]] = Nil
}
