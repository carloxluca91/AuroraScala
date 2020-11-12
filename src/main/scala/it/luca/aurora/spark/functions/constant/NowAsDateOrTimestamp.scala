package it.luca.aurora.spark.functions.constant

import java.sql.{Date, Timestamp}
import java.time.LocalDate

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

case class NowAsDateOrTimestamp(functionToApply: String)
  extends ConstantFunction(functionToApply, ConstantSignatures.nowAsDateOrTimestamp.regex) {

  override def getColumn: Column = {

    if (functionName endsWith "date") {

      lit(Date.valueOf(LocalDate.now()))
    } else {

      lit(new Timestamp(System.currentTimeMillis()))
    }
  }
}
