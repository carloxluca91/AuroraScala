package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName}
import it.luca.aurora.excel.bean.{SpecificationRow, SpecificationRows}
import it.luca.aurora.option.DataSourceLoadConfig
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

case class DataSourceLoadJob(override val sqlContext: SQLContext,
                             override val propertiesFile: String,
                             private val config: DataSourceLoadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.SourceLoad) {

  private val Latest = "LATEST"

  override protected val dataSource: Option[String] = Some(config.dataSource)
  override protected val dtBusinessDate: Option[String] = config.dtBusinessDate
  override protected val specificationVersion: Option[String] = Some(config.specificationVersion.getOrElse(Latest))

  // Hive properties
  private val db: String = jobProperties.getString("hive.db.trusted")
  private val specificationActual: String = jobProperties.getString("hive.table.specification.actual")
  private val specificationHistorical: String = jobProperties.getString("hive.table.specification.historical")

  private def specificationReadQuery(): String = {

    val specificationVersion: String = config.specificationVersion.getOrElse(Latest)
    if (specificationVersion.equals(Latest)) {
      s"""SELECT *
         |FROM $db.$specificationActual
         |WHERE TRIM(LOWER(data_source)) = '${config.dataSource}'
         |""".stripMargin
    } else {
      s"""SELECT *
         |FROM $db.$specificationHistorical
         |WHERE VERSION = '$specificationVersion'
         |AND TRIM(LOWER(data_source)) = '${config.dataSource}'
         |""".stripMargin
    }
  }

  private val decodeSpecificationDf: DataFrame => SpecificationRows = df => {

    import sqlContext.implicits._

    val beans: Seq[SpecificationRow] = df.withJavaNamingConvention()
      .as[SpecificationRow].collect().toSeq
    log.info(s"Decoded given DataFrame as ${beans.size} ${classOf[SpecificationRow].getSimpleName}(s)")
    SpecificationRows(beans)
  }

  private val getCleanDf: DataFrame => DataFrame = df => {

    val specificationRows = as[SpecificationRows]("SPECIFICATION_ROWS")
    val inputCheckCols: Seq[Column] = specificationRows.inputCheckColumns()
    df.filter(inputCheckCols.reduce(_ && _))
  }

  private val getErrorDf: DataFrame => DataFrame = df => {

    val specificationRows = as[SpecificationRows]("SPECIFICATION_ROWS")
    val inputCheckCols: Seq[Column] = specificationRows.inputCheckColumns()
    val checkDescriptionCols: Seq[Column] = specificationRows.inputChecks()
      .zip(inputCheckCols).map {
      case (str, check) => when(!check, lit(str))
    }

    // Filter given DataFrame in order to exclude records that do not satisfy a single input check
    // and add two columns reporting the number of failed checks and their description
    val failedCheckDescCol = concat_ws(", ", checkDescriptionCols: _*).as(ColumnName.FailedChecksDesc)
    val failedChecksNumCol = size(split(col(ColumnName.FailedChecksDesc), ", ")).as(ColumnName.FailedChecksNum)
    df.filter(inputCheckCols.map(!_).reduce(_ || _))
      .select(specificationRows.rwColumns ++ (failedChecksNumCol :: failedCheckDescCol :: Nil): _*)
      .withTechnicalColumns()
      .withSqlNamingConvention()
  }

  override protected val steps: Seq[Step[_]] = GetDfForQuery(specificationReadQuery(), "SPECIFICATION_DF", sqlContext) ::
    DfTo[SpecificationRows](as[DataFrame]("SPECIFICATION_DF"), "SPECIFICATION_ROWS", decodeSpecificationDf) ::
    new IOStep[SpecificationRows, String](as[SpecificationRows]("SPECIFICATION_ROWS"),
      "GET_RW_DATA_SOURCE",
      "RW_DATA_SOURCE") {
      override protected def stepFunction(input: SpecificationRows): String = input.rwDataSource
    } ::
    ReadHiveTable(s"$db.${as[String]("RW_DATA_SOURCE")}", "RW_DF", sqlContext) ::
    TransformDf(as[DataFrame]("RW_DF"), "RW_ERROR_DF", getErrorDf) ::
    TransformDf(as[DataFrame]("RW_DF"), "RW_CLEAN_DF", getCleanDf) :: Nil
}
