package it.luca.aurora

import org.apache.spark.sql.types.{DataType, DataTypes}

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import scala.reflect.runtime.universe.{TypeTag, typeOf}

package object utils {

  def classFullName[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.fullName

  def classSimpleName[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

  def now(): Timestamp = new Timestamp(System.currentTimeMillis())

  def toDate(timestamp: Timestamp): Date = new Date(timestamp.getTime)

  def toDate(timestamp: Timestamp, pattern: String): String = {

    new SimpleDateFormat(pattern).format(toDate(timestamp))
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
