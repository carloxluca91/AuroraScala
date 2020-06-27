package it.carloni.luca.aurora.utils

import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

object Utils {

  def getJavaSQLTimestampFromNow: java.sql.Timestamp =  {

    Timestamp.from(ZonedDateTime
      .now(ZoneId.of("Europe/Rome"))
      .toInstant)
  }

  def getJavaSQLDateFromNow: java.sql.Date = {

    new Date(ZonedDateTime
      .now(ZoneId.of("Europe/Rome"))
      .toInstant.toEpochMilli)

  }

}
