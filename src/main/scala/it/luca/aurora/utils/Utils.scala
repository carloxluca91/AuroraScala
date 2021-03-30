package it.luca.aurora.utils

import org.apache.spark.sql.types.{DataType, DataTypes}

import java.sql.{Date, Timestamp}

object Utils {

  def now: Timestamp = new Timestamp(System.currentTimeMillis())

  def toDate(timestamp: Timestamp): Date = new Date(timestamp.getTime)

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
