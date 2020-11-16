package it.luca.aurora.spark.data

import java.sql.{Date, Timestamp}

case class LogRecord(application_id: String,
                     application_name: String,
                     application_branch: String,
                     application_start_time: Timestamp,
                     application_start_date: Date,
                     application_end_time: Timestamp,
                     application_end_date: Date,
                     bancll_name: Option[String],
                     dt_riferimento: Option[Date],
                     target_database: String,
                     target_table: String,
                     exception_message: Option[String],
                     application_finish_code: Int,
                     application_finish_status: String)
