package it.luca.aurora.spark.job

import it.luca.aurora.core.utils.{now, toDate}
import it.luca.aurora.enumeration.{Branch, ColumnName, DateFormat}
import it.luca.aurora.excel.bean.{MappingRow, SpecificationRow}
import it.luca.aurora.option.ReloadConfig
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import org.apache.poi.ss.usermodel.Row
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

case class ReloadJob(override val sqlContext: SQLContext,
                     override val propertiesFile: String,
                     private val reloadConfig: ReloadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.Reload) {

  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  // Hive properties
  private val specificationHistorical: String = jobProperties.getString("hive.table.specification.historical")
  private val mappingHistorical: String = jobProperties.getString("hive.table.mapping.historical")

  private val retrieveVersion: DataFrame => String = df => {

    df.select(ColumnName.Version)
      .distinct().collect().head
      .getAs[String](0)
  }

  private val withValidityEndCols: DataFrame => DataFrame = df => {

    df.withColumnAfter(ColumnName.ValidityEndTime, lit(now()), ColumnName.ValidityStartDate)
      .withColumnAfter(ColumnName.ValidityEndDate, lit(toDate(now(), DateFormat.DateDefault)), ColumnName.ValidityEndTime)
      .withTechnicalColumns()
      .withSqlNamingConvention()
      .coalesce(1)
  }

  private def reloadSteps[T <: Product](actualTable: String, historicalTable: String, sheet: Int)
                                       (implicit typeTag: TypeTag[T], classTag: ClassTag[T], rowToT: Row => T): Seq[Step[_]] =

    ReadHiveTable(s"$trustedDb.$actualTable", isTableName = true, sqlContext, "OLD_VERSION_DF") ::
      DfTo[String]("OLD_VERSION_DF", retrieveVersion, "OLD_VERSION") ::
      TransformDf("OLD_VERSION_DF", withValidityEndCols, "OLD_VERSION_DF") ::
      WriteDf("OLD_VERSION_DF", trustedDb, historicalTable, isTableName = true,
        SaveMode.Append, Some(ColumnName.Version :: Nil), impalaJdbcConnection) ::
      ReadExcel(excelPath, "WORKBOOK") ::
      DecodeSheet[T]("WORKBOOK", sheet, "EXCEL_BEANS") ::
      ToDf[T]("EXCEL_BEANS", sqlContext, "EXCEL_BEANS_DF") ::
      UpdateDfVersion("EXCEL_BEANS_DF", "OLD_VERSION", "EXCEL_BEANS_DF") ::
      WriteDf("EXCEL_BEANS_DF", trustedDb, actualTable, isTableName = true,
        SaveMode.Overwrite, None, impalaJdbcConnection) :: Nil

  override protected val steps: Seq[Step[_]] = (if (reloadConfig.specificationFlag) {
    reloadSteps[SpecificationRow](specificationActual, specificationHistorical, specificationSheet)
  } else Nil) ++
    (if (reloadConfig.lookUpFlag) {
    reloadSteps[MappingRow](mappingActual, mappingHistorical, mappingSheet)
  } else Nil)
}
