package it.luca.aurora.spark.bean

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.step.{IOStep, Step}
import it.luca.aurora.utils.Utils.{now, toDate}
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
                     stepInputType: String,
                     stepOutputType: Option[String],
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
            step: Step[_],
            exceptionOpt: Option[Throwable]): LogRecord = {

    LogRecord(applicationName = sparkContext.appName,
      applicationBranch = branch.name,
      applicationStartTime = new Timestamp(sparkContext.startTime),
      applicationStartDate = new Date(sparkContext.startTime),
      dataSource = dataSource,
      dtBusinessDate = dtBusinessDate,
      specificationVersion = specificationVersion,
      stepIndex = stepIndex,
      stepName = step.stepName,
      stepInputType = step.stepInputType,
      stepOutputType = step match {
        case ioStep: IOStep[_, _] => Some(ioStep.stepOutputType)
        case _ => None
      },

      stepEndTime = now(),
      stepEndDate = toDate(now()),
      stepEndCode = exceptionOpt.map(_ => 0).getOrElse(-1),
      stepEndState = exceptionOpt.map(_ => "OK").getOrElse("KO"),
      stepExceptionClass = exceptionOpt.map(_.getClass.getName),
      stepExceptionMessage = exceptionOpt.map(_.getMessage)
    )
  }
}
