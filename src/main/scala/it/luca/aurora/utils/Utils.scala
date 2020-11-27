package it.luca.aurora.utils

import java.sql.{Date, Timestamp}
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.sql.types.{DataType, DataTypes}

object Utils {

  def getJavaSQLTimestampFromNow: Timestamp =  {

    Timestamp.from(ZonedDateTime
      .now(ZoneId.of("Europe/Rome"))
      .toInstant)
  }

  def getJavaSQLDateFromNow: Date = {

    new Date(ZonedDateTime
      .now(ZoneId.of("Europe/Rome"))
      .toInstant.toEpochMilli)
  }

  def insertElementAtIndex[T](s: Seq[T], element: T, index: Int): Seq[T] = {

    val (leftPart, rightPart): (Seq[T], Seq[T]) = s.splitAt(index)
    leftPart ++ Seq(element) ++ rightPart
  }

  def resolveDataType(columnType: String): DataType = {

    columnType.toLowerCase match {

      case "string" => DataTypes.StringType
      case "int" => DataTypes.IntegerType
      case "date" => DataTypes.DateType
      case "timestamp" => DataTypes.TimestampType
      case "double" => DataTypes.DoubleType
    }
  }
}
