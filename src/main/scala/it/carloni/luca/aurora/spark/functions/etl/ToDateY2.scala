package it.carloni.luca.aurora.spark.functions.etl

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{lit, udf}

import scala.util.{Failure, Success, Try}

case class ToDateY2(functionToApply: String)
  extends ETLFunction(functionToApply, ETLSignatures.toDateY2.regex) {

  private final val dateFormat: String = signatureMatch.group(4)
  private final val fromStringToDate: UserDefinedFunction = udf((date: String, dateFormat: String) => {

    Try {

      val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
      val localDateCurrentRecord: LocalDate = LocalDate.parse(date, dateTimeFormatter)
      if (localDateCurrentRecord.isAfter(LocalDate.now())) {

        localDateCurrentRecord.minusYears(100)
      } else {
        localDateCurrentRecord
      }
    } match {
      case Failure(_) => None
      case Success(value) => Some(value)
    }
  })

  override protected val transformationFunction: Column => Column = fromStringToDate(_, lit(dateFormat))

  override protected def toStringRepr: String = s"$functionName($nestedFunctionGroup3, date_format = '$dateFormat')"
}
