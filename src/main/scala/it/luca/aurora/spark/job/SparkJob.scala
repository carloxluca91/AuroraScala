package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.bean.LogRecord
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step.{IOStep, IStep, Step}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class SparkJob(protected val sqlContext: SQLContext,
                        protected val propertiesFile: String,
                        protected val branch: Branch.Value)
  extends Logging {

  protected final val jobProperties = new PropertiesConfiguration(propertiesFile)

  protected val dataSource: Option[String]
  protected val dtBusinessDate: Option[String]
  protected val specificationVersion: Option[String]
  protected val steps: Seq[Step[_]]

  private final val logRecordFunction: (Int, Step[_], Option[Throwable]) => LogRecord =
    LogRecord(sparkContext = sqlContext.sparkContext,
      branch = branch,
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      _: Int,
      _: Step[_],
      _: Option[Throwable])

  protected val jobVariables: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

  protected def as[T](key: String): T = jobVariables(key).asInstanceOf[T]

  def run(): Unit = {

    import sqlContext.implicits._

    val (jobSucceeded, logRecords): (Boolean, Seq[LogRecord]) = runSteps()
    val dbName: String = jobProperties.getString("hive.db.trusted")

    val logRecordsDf: DataFrame = logRecords.toDF()
    log.info(s"Turned ${logRecords.size} into a ${classOf[DataFrame].getSimpleName}")
    val logTableName = jobProperties.getString("hive.table.dataloadLog.name")
    logRecordsDf.withTechnicalColumns()
      .withSqlNamingConvention()
      .coalesce(1)
      .saveAsTableOrInsertInto(dbName, logTableName, SaveMode.Append, None)

    if (jobSucceeded) log.info(s"Executed whole Spark job ${branch.name}")
    else log.warn(s"Unable to fully execute Spark job ${branch.name}")
  }

  private def runSteps(): (Boolean, Seq[LogRecord]) = {

    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    for ((step, index) <- steps.zip(Stream.from(1))) {

      // Execute current step
      Try {
        // Pattern match on current step
        step match {
          case iOStep: IOStep[_, _] =>

            val (key, value) = iOStep.run()
            if (jobVariables.contains(key)) {
              log.warn(s"Key '$key' is already defined. It will be overwritten")
            } else {
              log.info(s"Defining new key '$key'")
            }
            jobVariables.update(key, value)
            log.info(s"Successfully updated jobVariables map")

          case iStep: IStep[_] => iStep.run()
          }
      } match {
        case Success(_) =>
          log.info(s"Executed step # $index (${step.stepName})")
          logRecords.append(logRecordFunction(index, step, None))

        case Failure(exception) =>
          log.error(s"Exception on step # $index (${step.stepName}). Stack trace: ", exception)
          logRecords.append(logRecordFunction(index, step, Some(exception)))
          return (false, logRecords)
      }
    }

    (true, logRecords)
  }
}