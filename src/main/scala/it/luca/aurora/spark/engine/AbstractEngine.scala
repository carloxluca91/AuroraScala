package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.bean.LogRecord
import it.luca.aurora.spark.step.{IOStep, IStep, Step}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class AbstractEngine(protected val sqlContext: SQLContext,
                              protected val propertiesFile: String,
                              protected val branch: Branch.Value)
  extends Logging {

  protected final val jobProperties = new PropertiesConfiguration(propertiesFile)
  protected final val dbName: String = jobProperties.getString("hive.db.trusted.name")

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

  private def updateJobVariableMap(key: String, value: Any): Unit = {

    if (jobVariables.contains(key)) {
      warn(s"Key '$key' is already defined. It will be overwritten")
    } else {
      info(s"Defining new key '$key'")
    }
    jobVariables(key) = value
    info(s"Successfully updated jobVariables map")
  }

  def run(): Unit = {

    val (jobSucceeded, logRecords): (Boolean, Seq[LogRecord]) = runSteps()

    import it.luca.aurora.spark.implicits._
    import sqlContext.implicits._

    val logRecordsDf: DataFrame = logRecords.toDF()
    info(s"Turned ${logRecords.size} into a ${classOf[DataFrame].getSimpleName}")
    val logTableName = jobProperties.getString("hive.table.dataloadLog.name")
    logRecordsDf.withSqlNamingConvention()
      .withTechnicalColumns()
      .coalesce(1)
      .saveAsTableOrInsertInto(dbName, logTableName, SaveMode.Append, None)

    if (jobSucceeded) info(s"Executed whole Spark job ${branch.name}")
    else warn(s"Unable to fully execute Spark job ${branch.name}")
  }

  private def runSteps(): (Boolean, Seq[LogRecord]) = {

    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    for ((step, index) <- steps.zip(Stream.from(1))) {

      Try {
        // Pattern match on current step
        step match {
          case iOStep: IOStep[_, _] => val (key, value) = iOStep.run()
            updateJobVariableMap(key, value)
          case iStep: IStep[_] => iStep.run()
          }
      } match {
        case Success(_) =>

          info(s"Executed step # $index (${step.stepName})")
          logRecords.append(logRecordFunction(index, step, None))

        case Failure(exception) =>

          error(s"Exception on step # $index (${step.stepName}). Stack trace: ", exception)
          logRecords.append(logRecordFunction(index, step, Some(exception)))
          return (false, logRecords)
      }
    }

    (true, logRecords)
  }
}