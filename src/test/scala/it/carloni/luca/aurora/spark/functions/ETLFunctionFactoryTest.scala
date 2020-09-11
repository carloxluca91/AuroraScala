package it.carloni.luca.aurora.spark.functions

import it.carloni.luca.aurora.spark.exception.UnmatchedFunctionException
import it.carloni.luca.aurora.utils.Utils.fullyMatchColOrLit
import org.apache.log4j.Logger
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

class ETLFunctionFactoryTest extends FunSuite {

  private final val logger = Logger.getLogger(getClass)

  test("ColOrLit") {

    val sFullMatch: String = "col('abc')"
    val sPartialMatch: String = s"a$sFullMatch"

    // TEST FULL SIGNATURE MATCH
    assertResult(true)(fullyMatchColOrLit(sFullMatch))
    assertResult(false)(fullyMatchColOrLit(sPartialMatch))
  }

  test(s"${classOf[DateFormatFunction].getSimpleName}") {

    val str: String = "date_format(lpad(@, 5, '0'), 'yyyy-MM-dd', 'dd/MM/yyyy')"
    val dateFormatFunction: DateFormatFunction = DateFormatFunction(str)
    assertResult(true)(dateFormatFunction.hasNestedFunction)

    val leftOrRightPadFunction: LeftOrRightPadFunction = LeftOrRightPadFunction(dateFormatFunction.nestedFunctionGroup3)
    assertResult(false)(leftOrRightPadFunction.hasNestedFunction)
  }

  test(s"${classOf[LeftOrRightConcatWsFunction].getSimpleName}") {

    val stringToMatch: String = "lconcat_ws(@, col('data'), ' ')"
    val column: Column = ETLFunctionFactory(stringToMatch, col("ora"))
    logger.info(s"Column definition: ${column.toString}")
  }


  test(s"${classOf[ToDateOrTimestampFunction].getSimpleName}") {

    val stringToMatch: String = "to_timestamp(lconcat_ws(@, col('data'), ' '), 'dd/MM/yyyy HH:mm:ss')"
    val column: Column = ETLFunctionFactory(stringToMatch, col("ora"))
    logger.info(s"Column definition: ${column.toString}")
  }

  test(s"${classOf[LeftOfRightConcatFunction].getSimpleName}") {

    val firstTestMap: Map[String, Boolean] = Map("lconcat(@, lit('abc'))" -> false,
      "lconcat(lpad(@, 5, 'a'), lit('abc'))" -> true,
      "lconcat(@, col('column_name'))" -> false,
      "lconcat(lpad(@, 5, 'a'), col('column_name'))" -> true)

    val processFirstTestMapEntry: (String, Boolean) => Unit = (string, assertion) => {

      logger.info(s"String to match: '$string'. Assertion value = $assertion")
      val function: LeftOfRightConcatFunction = LeftOfRightConcatFunction(string)
      assertResult(assertion)(function.hasNestedFunction)
    }

    // TEST LPAD
    firstTestMap.foreach(x => processFirstTestMapEntry(x._1, x._2))

    // TEST RPAD
    firstTestMap.map(x => (x._1.replaceFirst("l", "r"), x._2))
      .foreach(x => processFirstTestMapEntry(x._1, x._2))

    // (STRING TO MATCH, THROWS EXCEPTION, HAS NESTED f(.))
    val secondTestMap: Map[String, (Boolean, Boolean)] = Map("lconcat(@, lpad(col('data_movimento'), 10, '5'))" -> (false, false),
      "lconcat(date_format(@, 'yyyy/MM/dd'), lpad(col('data_movimento'), 10, '5'))" -> (false, true),
      "lconcat(@, x(col('data_movimento'), 10, '5'))" -> (true, false),
      "lconcat(date_format(@, 'yyyy/MM/dd'), x(col('data_movimento'), 10, '5'))" -> (true, true))

    val processSecondTestMapEntry: String => Unit = stringToMatch => {

      logger.info(s"String to match '$stringToMatch'. Expecting exception of type ${classOf[UnmatchedFunctionException].getSimpleName}")
      intercept[UnmatchedFunctionException](LeftOfRightConcatFunction(stringToMatch))
    }

    secondTestMap.filter(x => x._2._1)
      .foreach(x => processSecondTestMapEntry(x._1))

    secondTestMap.filter(x => !x._2._1)
      .foreach(x => processFirstTestMapEntry(x._1, x._2._2))
  }
}
