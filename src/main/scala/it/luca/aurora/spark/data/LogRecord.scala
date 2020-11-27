package it.luca.aurora.spark.data

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import it.luca.aurora.utils.{DateFormat, Utils}
import org.apache.spark.SparkContext

case class LogRecord(application_id: String,
                     application_name: String,
                     application_branch: String,
                     application_start_time: Timestamp,
                     application_start_date: Date,
                     step_finish_time: Timestamp,
                     step_finish_date: Date,
                     bancll_name: Option[String],
                     dt_riferimento: Option[Date],
                     target_database: String,
                     target_table: String,
                     exception_message: Option[String],
                     application_finish_code: Int,
                     application_finish_status: String)

object LogRecord {

  def apply(sparkContext: SparkContext, branchName: String, bancllNameOpt: Option[String], dtRiferimentoOpt: Option[String],
            targetDatabase: String, targetTable: String, exceptionMsgOpt: Option[String]): LogRecord = {

    val applicationId: String = sparkContext.applicationId
    val applicationName: String = sparkContext.appName

    val dtRiferimentoSQLDateOpt: Option[Date] = dtRiferimentoOpt match {
      case None => None
      case Some(x) => Some(Date.valueOf(LocalDate.parse(x, DateFormat.DtRiferimento.formatter)))
    }

    val applicationStartTime: Timestamp = Timestamp.from(Instant.ofEpochMilli(sparkContext.startTime))
    val applicationStartDate: Date = new Date(sparkContext.startTime)
    val applicationEndTime: Timestamp = Utils.getJavaSQLTimestampFromNow
    val applicationEndDate: Date = Utils.getJavaSQLDateFromNow
    val applicationFinishCode: Int = if (exceptionMsgOpt.isEmpty) 0 else -1
    val applicationFinishStatus: String = if (exceptionMsgOpt.isEmpty) "OK" else "KO"

    LogRecord(applicationId,
      applicationName,
      branchName,
      applicationStartTime,
      applicationStartDate,
      applicationEndTime,
      applicationEndDate,
      bancllNameOpt,
      dtRiferimentoSQLDateOpt,
      targetDatabase,
      targetTable,
      exceptionMsgOpt,
      applicationFinishCode,
      applicationFinishStatus)
  }
}
