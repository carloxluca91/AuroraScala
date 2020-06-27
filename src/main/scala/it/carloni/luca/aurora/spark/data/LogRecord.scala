package it.carloni.luca.aurora.spark.data

case class LogRecord(application_id: String,
                     application_name: String,
                     application_branch: String,
                     application_start_time: java.sql.Timestamp,
                     application_end_time: java.sql.Timestamp,
                     bancll_name: Option[String],
                     dt_business_date: Option[java.sql.Date],
                     impacted_table: String,
                     exception_message: Option[String],
                     application_finish_code: Int,
                     application_finish_status: String)
