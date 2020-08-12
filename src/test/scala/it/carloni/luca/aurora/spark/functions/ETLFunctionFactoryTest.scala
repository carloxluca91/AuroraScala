package it.carloni.luca.aurora.spark.functions

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class ETLFunctionFactoryTest extends FunSuite {

  private final val logger = Logger.getLogger(getClass)

  test("DateFormat string match") {

    val str = "date_format(lpad(@, 5, '0'), 'yyyy-MM-dd', 'dd/MM/yyyy')"
    ETLFunctionFactory(str, lit("2020-01-01"))
  }
}
