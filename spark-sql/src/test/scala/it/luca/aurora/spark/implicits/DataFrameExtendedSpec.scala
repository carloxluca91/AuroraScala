package it.luca.aurora.spark.implicits

import it.luca.aurora.CustomSpec
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll

case class TestDfBean(x: Int, y: String)

class DataFrameExtendedSpec
  extends CustomSpec
    with BeforeAndAfterAll {

  private val sparkConf = new SparkConf()
    .setAppName(s"${className[DataFrameExtendedSpec]} Suite")
    .setMaster("local[*]")

  private val sparkContext = new SparkContext(sparkConf)
  private val sqlContext = new SQLContext(sparkContext)
  private val (firstColName, secondColName) = ("applicationStartTime", "applicationEndTime")
  private val (newColName, newCol) = ("newCol", lit("world"))
  private val testDf = sqlContext
    .createDataFrame(TestDfBean(1, "hello") :: Nil)
    .toDF(firstColName, secondColName)

  override protected def afterAll(): Unit = sparkContext.stop()

  s"A ${className[DataFrameExtended]}" must "insert a new column after a given column" in {

    val newDf = testDf.withColumnAfter(newColName, newCol, firstColName)
    assertResult(testDf.columns.length + 1) {
      newDf.columns.length
    }
    assertResult(testDf.columns.indexOf(firstColName) + 1) {
      newDf.columns.indexOf(newColName)
    }
  }

  it must "insert a new column before a given column" in {

    val newDf = testDf.withColumnBefore(newColName, newCol, secondColName)
    assertResult(testDf.columns.length + 1) {
      newDf.columns.length
    }
    assertResult(testDf.columns.indexOf(secondColName)) {
      newDf.columns.indexOf(newColName)
    }
  }

  it must "rename all columns according to SQL naming convention" in {

    val newDf = testDf.withSqlNamingConvention()
    val regex = "([A-Z])".r
    testDf.columns.zip(newDf.columns)
      .foreach { case (oldName, newName) =>
      assert(regex.replaceAllIn(oldName, m => s"_${m.group(1).toLowerCase}").equals(newName))
      }
  }

  it must "rename all columns according to Java naming convention" in {

    val newDf = testDf.withSqlNamingConvention()
      .withJavaNamingConvention()
    testDf.columns.zip(newDf.columns)
      .foreach { case (oldName, newName) =>
        assert(oldName.equals(newName))
      }
  }
}
