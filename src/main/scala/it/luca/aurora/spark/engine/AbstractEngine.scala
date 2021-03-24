package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.data.LogRecord
import it.luca.aurora.spark.step.{AbstractStep, CreateDbStep}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class AbstractEngine(protected val sqlContext: SQLContext,
                              protected val propertiesFile: String)
  extends Logging {

  protected final val jobProperties = new PropertiesConfiguration(propertiesFile)
  protected final val dbName: String = jobProperties.getString("hive.db.name")

  protected val branch: Branch.Value
  protected val dataSource: Option[String]
  protected val dtBusinessDate: Option[String]
  protected val specificationVersion: Option[String]
  protected val steps: Seq[(AbstractStep[_, _], _)]

  protected final val logRecordFunction: (Int, AbstractStep[_, _], Option[Throwable]) => LogRecord =
    LogRecord(sparkContext = sqlContext.sparkContext,
      branch = branch,
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      _: Int,
      _: AbstractStep[_, _],
      _: Option[Throwable])


  def run(): Unit = {

    val (jobSucceeded, logRecords): (Boolean, Seq[LogRecord]) = runSteps()

    import sqlContext.implicits._
    import it.luca.aurora.spark.implicits._

    val logRecordsBeanDf: DataFrame = logRecords.toDF()
    info(s"Turned ${logRecords.size} into a ${classOf[DataFrame].getSimpleName}")
    val regex: util.matching.Regex = "([A-Z])".r
    val logRecordDf: DataFrame = logRecordsBeanDf.columns
      .foldLeft(logRecordsBeanDf) { case (df, columnName) =>

        val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
        df.withColumnRenamed(columnName, newColumnName)
      }

    val logTableName = jobProperties.getString("hive.logTable.name")
    logRecordDf
      .withTechnicalColumns()
      .coalesce(1)
      .saveAsTableOrInsertInto(dbName, logTableName, SaveMode.Append, None)

    if (jobSucceeded) info(s"Executed whole Spark job ${branch.name}")
    else warn(s"Unable to fully execute Spark job ${branch.name}")
  }

  private def runSteps(): (Boolean, Seq[LogRecord]) = {

    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    for (((step, input), index) <- steps.zip(Stream.from(1))) {

      Try {
        // Pattern match on current step
        step match {
          case createDb: CreateDbStep => createDb.run(input.asInstanceOf[String])
          case _ =>
        }
      } match {
        case Success(_) =>

          // Add "positive" LogRecord
          info(s"Executed step # $index (${step.name})")
          logRecords.append(logRecordFunction(index, step, None))

        case Failure(exception) =>

          // Add "negative" LogRecord and return
          error(s"Exception on step # $index (${step.name}). Stack trace: ", exception)
          logRecords.append(logRecordFunction(index, step, Some(exception)))
          return (false, logRecords)
      }
    }

    (true, logRecords)
  }
}