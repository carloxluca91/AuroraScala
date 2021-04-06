package it.luca.aurora.spark.job

import it.luca.aurora.core.Logging
import it.luca.aurora.core.utils.classSimpleName
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.bean.LogRecord
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step.{IOStep, IStep, Step}
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.sql.{Connection, DriverManager}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

abstract class SparkJob(val sqlContext: SQLContext,
                        val propertiesFile: String,
                        val branch: Branch.Value)
  extends Logging {

  protected final val jobProperties = new PropertiesConfiguration(propertiesFile)

  protected val dataSource: Option[String]
  protected val dtBusinessDate: Option[String]
  protected val specificationVersion: Option[String]
  protected val steps: Seq[Step[_]]

  // Excel properties
  protected final val excelPath: String = jobProperties.getString("excel.hdfs.path")
  protected final val specificationSheet: Int = jobProperties.getInt("excel.specification.sheet")
  protected final val mappingSheet: Int = jobProperties.getInt("excel.mapping.sheet")

  // Hive properties
  protected final val trustedDb: String = jobProperties.getString("hive.db.trusted")
  protected final val specificationActual: String = jobProperties.getString("hive.table.specification.actual")
  protected final val mappingActual: String = jobProperties.getString("hive.table.mapping.actual")

  // Impala JDBC connection
  protected val impalaJdbcConnection: Connection = {

    val driverClass = jobProperties.getString("impala.jdbc.driverClass")
    val impalaUrl = jobProperties.getString("impala.jdbc.url")
    Class.forName (driverClass)
    val connection = DriverManager.getConnection (impalaUrl)
    log.info (s"Connected to JDBC Url $impalaUrl using driver $driverClass")
    connection
  }

  private final val logRecordForStep: (Int, Step[_], Option[Throwable]) => LogRecord =
    LogRecord(sparkContext = sqlContext.sparkContext,
      branch = branch,
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      _: Int,
      _: Step[_],
      _: Option[Throwable],
      yarnUIUrl = jobProperties.getString("yarn.ui.url"))

  private val jobVariables: mutable.Map[String, Any] = mutable.Map.empty[String, Any]

  def run(): Unit = {

    import sqlContext.implicits._

    // Convert the sequence of LogRecords to a DataFrame
    val (jobSucceeded, logRecords): (Boolean, Seq[LogRecord]) = runSteps()
    val logRecordsDf: DataFrame = logRecords.toDF()
    log.info(s"Turned ${logRecords.size} ${classSimpleName[LogRecord]}(s) into a ${classOf[DataFrame].getSimpleName}")

    // Insert LogRecords DataFrame
    val dbName: String = jobProperties.getString("hive.db.trusted")
    val logTableName: String = jobProperties.getString("hive.table.dataloadLog.name")
    sqlContext.createDbIfNotExists(dbName)
    logRecordsDf.withTechnicalColumns()
      .withSqlNamingConvention()
      .coalesce(1)
      .saveAsOrInsertInto(dbName, logTableName, SaveMode.Append, None, impalaJdbcConnection)

    if (jobSucceeded) log.info(s"Executed whole Spark job ${branch.name}")
    else log.warn(s"Unable to fully execute Spark job ${branch.name}")
  }

  private def runSteps(): (Boolean, Seq[LogRecord]) = {

    val logRecords: mutable.ListBuffer[LogRecord] = mutable.ListBuffer.empty[LogRecord]
    for ((step, index) <- steps.zip(Stream.from(1))) {

      // Execute current step
      Try {
        step match {
          case iOStep: IOStep[_, _] =>
            val (key, value) = iOStep.run(jobVariables)
            if (jobVariables.contains(key)) {
              log.warn(s"Key '$key' is already defined. It will be overwritten")
            } else {
              log.info(s"Defining new key '$key'")
            }
            jobVariables.update(key, value)
            log.info(s"Successfully updated jobVariables map")

          case iStep: IStep[_] => iStep.run(jobVariables)
          }
      } match {
        case Success(_) =>
          log.info(s"Executed step # $index (${step.description})")
          logRecords.append(logRecordForStep(index, step, None))

        case Failure(exception) =>
          log.error(s"Exception on step # $index (${step.description}). Stack trace: ", exception)
          logRecords.append(logRecordForStep(index, step, Some(exception)))
          return (false, logRecords)
      }
    }

    (true, logRecords)
  }
}