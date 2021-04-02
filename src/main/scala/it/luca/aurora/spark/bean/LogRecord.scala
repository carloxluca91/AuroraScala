package it.luca.aurora.spark.bean

import it.luca.aurora.enumeration.{Branch, DateFormat}
import it.luca.aurora.spark.step.{IOStep, Step}
import it.luca.aurora.utils.{now, toDate}
import org.apache.spark.SparkContext

import java.sql.Timestamp

case class LogRecord(applicationName: String,
                     applicationBranch: String,
                     applicationStartTime: Timestamp,
                     applicationStartDate: String,
                     dataSource: Option[String],
                     dtBusinessDate: Option[String],
                     specificationVersion: Option[String],
                     stepIndex: Int,
                     stepClassName: String,
                     stepDescription: String,
                     stepInputType: String,
                     stepOutputType: Option[String],
                     stepEndTime: Timestamp,
                     stepEndDate: String,
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
            step: Step[_],
            exceptionOpt: Option[Throwable]): LogRecord = {

    val applicationStartTime = new Timestamp(sparkContext.startTime)
    LogRecord(applicationName = sparkContext.appName,
      applicationBranch = branch.name,
      applicationStartTime = applicationStartTime,
      applicationStartDate = toDate(applicationStartTime, DateFormat.DateDefault),
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      stepIndex = stepIndex,
      stepClassName = step.stepClassName,
      stepDescription = step.description,
      stepInputType = step.stepInputType,
      stepOutputType = step match {
        case ioStep: IOStep[_, _] => Some(ioStep.stepOutputType)
        case _ => None
      },

      stepEndTime = now(),
      stepEndDate = toDate(now(), DateFormat.DateDefault),
      stepEndCode = exceptionOpt.map(_ => -1).getOrElse(0),
      stepEndState = exceptionOpt.map(_ => "KO").getOrElse("OK"),
      stepExceptionClass = exceptionOpt.map(_.getClass.getName),
      stepExceptionMessage = exceptionOpt.map(_.getMessage)
    )
  }
}
