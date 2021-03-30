package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName}
import it.luca.aurora.excel.bean.{LookupRow, SpecificationRow}
import it.luca.aurora.option.ReloadConfig
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import it.luca.aurora.utils.Utils.{now, toDate}
import org.apache.poi.ss.usermodel.{Row, Workbook}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class ReloadJob(override protected val sqlContext: SQLContext,
                     override protected val propertiesFile: String,
                     protected val reloadConfig: ReloadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.Reload) {

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
  private val specificationHistorical: String = jobProperties.getString("hive.table.specification.historical")
  private val lookupActual: String = jobProperties.getString("hive.table.lookup.actual")
  private val lookupHistorical: String = jobProperties.getString("hive.table.lookup.historical")

  private val addValidityEndColumns: DataFrame => DataFrame = df => {

    df.withColumnAfter(ColumnName.ValidityEndTime, lit(now), ColumnName.ValidityStartDate)
      .withColumnAfter(ColumnName.ValidityEndDate, lit(toDate(now)), ColumnName.ValidityEndTime)
      .withTechnicalColumns()
      .withSqlNamingConvention()
      .coalesce(1)
  }

  private val beansDfTransformation: (DataFrame, String) => DataFrame = (df, oldVersion) => {

    val updatedVersion = f"${oldVersion.toDouble + 0.1}%.1f".replace(",", ".")
    log.info(s"Updating version from $oldVersion to $updatedVersion")
    df.withColumn(ColumnName.ValidityStartTime, lit(now))
      .withColumn(ColumnName.ValidityStartDate, lit(toDate(now)))
      .withTechnicalColumns()
      .withColumn(ColumnName.Version, lit(updatedVersion))
      .withSqlNamingConvention()
      .coalesce(1)
  }

  private val retrieveDfVersion: DataFrame => String = df => {

    df.select(ColumnName.Version)
      .distinct().collect().head
      .getAs[String](0)
  }

  private def reloadSteps[T <: Product](actualTable: String, historicalTable: String, sheet: Int)
                                       (implicit rowToT: Row => T): Seq[Step[_]] =

    ReadHiveTable(s"$db.$actualTable", "OLD_VERSION_DF", sqlContext) ::
      FromDfTo[String](as[DataFrame]("OLD_VERSION_DF"), "OLD_VERSION", retrieveDfVersion) ::
      TransformDf(as[DataFrame]("OLD_VERSION_DF"), "OLD_VERSION_DF", addValidityEndColumns) ::
      WriteDf(as[DataFrame]("OLD_VERSION_DF"), db, historicalTable, SaveMode.Append, Some(ColumnName.Version :: Nil)) ::
      ReadExcel(excelPath, "WORKBOOK") ::
      DecodeSheet[T](as[Workbook]("WORKBOOK"), "EXCEL_BEANS", sheet) ::
      ToDf[T](as[Seq[T]]("EXCEL_BEANS"), "EXCEL_BEANS_DF", sqlContext) ::
      TransformDf(as[DataFrame]("EXCEL_BEANS_DF"), "EXCEL_BEANS_DF", beansDfTransformation(_: DataFrame, as[String]("OLD_VERSION"))) ::
      WriteDf(as[DataFrame]("SPECIFICATION_DF"), db, actualTable, SaveMode.Overwrite, None) :: Nil


  override protected val steps: Seq[Step[_]] = (if (reloadConfig.specificationFlag) {
    reloadSteps[SpecificationRow](specificationActual, specificationHistorical, specificationSheet)
  } else Nil) ++
    (if (reloadConfig.lookUpFlag) {
    reloadSteps[LookupRow](lookupActual, lookupHistorical, lookupSheet)
  } else Nil)
}
