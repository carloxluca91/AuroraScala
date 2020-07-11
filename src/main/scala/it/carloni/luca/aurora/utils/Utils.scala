package it.carloni.luca.aurora.utils

import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.types.{DataType, DataTypes}

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

  def resolveDataType(columnType: String): DataType = {

    columnType.toLowerCase match {

      case "string" => DataTypes.StringType
      case "int" => DataTypes.IntegerType
      case "date" => DataTypes.DateType
      case "timestamp" => DataTypes.TimestampType
    }
  }
}
