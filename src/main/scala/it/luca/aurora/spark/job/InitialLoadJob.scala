package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName, DateFormat}
import it.luca.aurora.excel.bean.{LookupRow, SpecificationRow}
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import it.luca.aurora.utils.{now, toDate}
import org.apache.poi.ss.usermodel.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class InitialLoadJob(override val sqlContext: SQLContext,
                          override val propertiesFile: String)
  extends SparkJob(sqlContext, propertiesFile, Branch.InitialLoad) {

  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  private val withValidityStartCols: DataFrame => DataFrame = df => {

    df.withColumn(ColumnName.ValidityStartTime, lit(now()))
      .withColumn(ColumnName.ValidityStartDate, lit(toDate(now(), DateFormat.DateDefault)))
      .withTechnicalColumns()
      .withColumn(ColumnName.Version, lit("0.1"))
      .withSqlNamingConvention()
      .coalesce(1)
  }

  private def initialLoadSteps[T <: Product](sheet: Int, actualTable: String)
                                            (implicit typeTag: TypeTag[T], classTag: ClassTag[T], rowToT: Row => T): Seq[Step[_]] =

    DecodeSheet[T]("WORKBOOK", sheet, "EXCEL_BEANS") ::
      ToDf[T]("EXCEL_BEANS", sqlContext, "EXCEL_BEANS_DF") ::
      TransformDf("EXCEL_BEANS_DF", withValidityStartCols, "EXCEL_BEANS_DF") ::
      WriteDf("EXCEL_BEANS_DF", trustedDb, actualTable, isTableName = true, SaveMode.Overwrite, Some(ColumnName.Version :: Nil)) :: Nil


  override protected val steps: Seq[Step[_]] = CreateDbIfNotExists(trustedDb, sqlContext) ::
    ReadExcel(excelPath, "WORKBOOK") :: Nil ++
    initialLoadSteps[SpecificationRow](specificationSheet, specificationActual) ++
    initialLoadSteps[LookupRow](lookupSheet, lookupActual)
}

