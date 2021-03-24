package it.luca.aurora.spark.data

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.step.AbstractStep
import org.apache.spark.SparkContext

import java.sql.{Date, Timestamp}

case class LogRecord(applicationName: String,
                     applicationBranch: String,
                     applicationStartTime: Timestamp,
                     applicationStartDate: Date,
                     dataSource: Option[String],
                     dtBusinessDate: Option[String],
                     specificationVersion: Option[String],
                     stepIndex: Int,
                     stepName: String,
                     stepEndTime: Timestamp,
                     stepEndDate: Date,
                     stepEndCode: Int,
                     stepEndState: String,
                     stepExceptionClass: Option[String],
                     stepExceptionMessage: Option[String])

object LogRecord {

  def apply(sparkContext: SparkContext,
            branch: Branch.Value,
            dataSource: Option[String],
            dtBusinessDate: Option[String],
            specificationVersion: Option[String],
            stepIndex: Int,
            step: AbstractStep[_, _],
            exceptionOpt: Option[Throwable]): LogRecord = {

    LogRecord(applicationName = sparkContext.appName,
      applicationBranch = branch.name,
      applicationStartTime = new Timestamp(sparkContext.startTime),
      applicationStartDate = new Date(sparkContext.startTime),
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      stepIndex = stepIndex,
      stepName = step.name,
      stepEndTime = new Timestamp(System.currentTimeMillis()),
      stepEndDate = new Date(System.currentTimeMillis()),
      stepEndCode = exceptionOpt.map(_ => 0).getOrElse(-1),
      stepEndState = exceptionOpt.map(_ => "OK").getOrElse("KO"),
      stepExceptionClass = exceptionOpt.map(_.getClass.getName),
      stepExceptionMessage = exceptionOpt.map(_.getMessage)
    )
  }
}
