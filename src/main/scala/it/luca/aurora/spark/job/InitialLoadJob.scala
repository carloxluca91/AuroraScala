package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName}
import it.luca.aurora.excel.bean.{LookupRow, SpecificationRow}
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import it.luca.aurora.utils.Utils.{now, toDate}
import org.apache.poi.ss.usermodel.{Row, Workbook}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class InitialLoadJob(override protected val sqlContext: SQLContext,
                          override protected val propertiesFile: String)
  extends SparkJob(sqlContext, propertiesFile, Branch.InitialLoad) {

  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  // Excel properties
  private val excelPath: String = jobProperties.getString("excel.hdfs.path")
  private val specificationSheet: Int = jobProperties.getInt("excel.specification.sheet")
  private val lookupSheet: Int = jobProperties.getInt("excel.lookup.sheet")

  // Hive properties
  private val db: String = jobProperties.getString("hive.db.trusted")
  private val specificationActual: String = jobProperties.getString("hive.table.specification.actual")
  private val lookupActual: String = jobProperties.getString("hive.table.lookup.actual")

  private val beansDfTransformation: DataFrame => DataFrame = df => {

    df.withColumn(ColumnName.ValidityStartTime, lit(now))
      .withColumn(ColumnName.ValidityStartDate, lit(toDate(now)))
      .withTechnicalColumns()
      .withColumn(ColumnName.Version, lit("0.1"))
      .withSqlNamingConvention()
      .coalesce(1)
  }

  private def initialLoadSteps[T <: Product](sheet: Int, actualTable: String)(implicit rowToT: Row => T): Seq[Step[_]] =

    DecodeSheet[T](as[Workbook]("WORKBOOK"), "EXCEL_BEANS", sheet) ::
      ToDf[T](as[Seq[T]]("EXCEL_BEANS"), "EXCEL_BEANS_DF", sqlContext) ::
      TransformDf(as[DataFrame]("EXCEL_BEANS_DF"), "EXCEL_BEANS_DF", beansDfTransformation) ::
      WriteDf(as[DataFrame]("EXCEL_BEANS_DF"), db, actualTable, SaveMode.Overwrite, None) :: Nil


  override protected val steps: Seq[Step[_]] = CreateDbIfNotExists(db, sqlContext) ::
    ReadExcel(excelPath, "WORKBOOK") :: Nil ++
    initialLoadSteps[SpecificationRow](specificationSheet, specificationActual) ++
    initialLoadSteps[LookupRow](lookupSheet, lookupActual)
}

