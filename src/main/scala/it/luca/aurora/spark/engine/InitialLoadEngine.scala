package it.luca.aurora.spark.engine

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.excel.bean.SpecificationRow
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import org.apache.poi.ss.usermodel.Workbook
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import java.sql.{Date, Timestamp}

case class InitialLoadEngine(override protected val sqlContext: SQLContext, 
                             override protected val propertiesFile: String)
  extends AbstractEngine(sqlContext, propertiesFile, Branch.InitialLoad) {

  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  private val excelPath = jobProperties.getString("hdfs.excel.path")
  private val specificationTableName = jobProperties.getString("hive.table.specification.name")
  private val specificationSheetIndex: Int = jobProperties.getInt("excel.specification.sheet")
  private val transformSpecificationDf: DataFrame => DataFrame = df => {

    df.withColumn("ts_validity_start", lit(new Timestamp(System.currentTimeMillis())))
      .withColumn("dt_validity_start", lit(new Date(System.currentTimeMillis())))
      .withTechnicalColumns()
      .withColumn("version", lit("0.1"))
      .withSqlNamingConvention()
      .coalesce(1)
  }

  override protected val steps: Seq[Step[_]] = CreateDb(dbName, sqlContext) ::
    ReadExcel(excelPath, "WORKBOOK") ::
    DecodeSheet[SpecificationRow](as[Workbook]("WORKBOOK"), "SPECIFICATION_ROWS", specificationSheetIndex, skipHeader = true) ::
    ToDf[SpecificationRow](as[Seq[SpecificationRow]]("SPECIFICATION_ROWS"), "SPECIFICATION_DF", sqlContext) ::
    TransformDf(as[DataFrame]("SPECIFICATION_DF"), "SPECIFICATION_DF", transformSpecificationDf) ::
    WriteDf(as[DataFrame]("SPECIFICATION_DF"), dbName, specificationTableName, SaveMode.ErrorIfExists, None) :: Nil
}

