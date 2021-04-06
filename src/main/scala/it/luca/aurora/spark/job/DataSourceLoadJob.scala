package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName}
import it.luca.aurora.excel.bean.{SpecificationRow, SpecificationRows}
import it.luca.aurora.option.DataSourceLoadConfig
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}

import scala.collection.mutable

case class DataSourceLoadJob(override val sqlContext: SQLContext,
                             override val propertiesFile: String,
                             private val config: DataSourceLoadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.DataSourceLoad) {

  private val Latest = "LATEST"

  override protected val dataSource: Option[String] = Some(config.dataSource)
  override protected val dtBusinessDate: Option[String] = config.dtBusinessDate
  override protected val specificationVersion: Option[String] = Some(config.specificationVersion.getOrElse(Latest))

  // Hive properties
  private val specificationHistorical: String = jobProperties.getString("hive.table.specification.historical")
  private val rawDb: String = jobProperties.getString("hive.db.raw")

  private def specificationReadQuery(): String = {

    if (specificationVersion.equals(Some(Latest))) {
      s"""SELECT *
         |FROM $trustedDb.$specificationActual
         |WHERE TRIM(LOWER(data_source)) = '${config.dataSource}'
         |""".stripMargin
    } else {
      s"""SELECT *
         |FROM $trustedDb.$specificationHistorical
         |WHERE VERSION = '$specificationVersion'
         |AND TRIM(LOWER(data_source)) = '${config.dataSource}'
         |""".stripMargin
    }
  }

  private val decodeSpecificationDf: DataFrame => SpecificationRows = df => {

    import sqlContext.implicits._

    val beans: Seq[SpecificationRow] = df.withJavaNamingConvention()
      .as[SpecificationRow].collect().toSeq
    val (beanSize, beanClassName) = (beans.size, classOf[SpecificationRow].getSimpleName)
    log.info(s"Decoded given DataFrame as $beanSize $beanClassName(s)")
    SpecificationRows(beans)
  }

  private val trustedDfTransformation: (DataFrame, SpecificationRows) => DataFrame = (df, specificationRows) => {

    val inputCheckCols: Seq[Column] = specificationRows.inputCheckColumns()
    val trdColumns: Seq[Column] = specificationRows.trdColumns
      .sortBy(_._1).map { case (_, str, column) => column.as(str) }

    df.filter(inputCheckCols.reduce(_ && _))
      .select(trdColumns: _*)
      .withTechnicalColumns()
      .withSqlNamingConvention()
  }

  private val errorDfTransformation: (DataFrame, SpecificationRows) => DataFrame = (df, specificationRows) => {

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

  override protected val steps: Seq[Step[_]] = GetDfForQuery(specificationReadQuery(), sqlContext, "SPECIFICATION_DF") ::
    DfTo[SpecificationRows]("SPECIFICATION_DF", decodeSpecificationDf, "SPECIFICATION_ROWS") ::
    new IOStep[SpecificationRows, String]("GET_RW_DATA_SOURCE", "RW_DATA_SOURCE") {

      override def run(variables: mutable.Map[String, Any]): (String, String) = {
        val specificationRows = variables("SPECIFICATION_ROWS").asInstanceOf[SpecificationRows]
        (outputKey, s"$rawDb.aaa")
      }
    } ::
    ReadHiveTable("RW_DATA_SOURCE", isTableName = false, sqlContext, "RW_DF") ::
    TransformDfUsingSpecifications("RW_DF", "SPECIFICATION_ROWS", errorDfTransformation, "RW_ERROR_DF") ::
    TransformDfUsingSpecifications("RW_DF", "SPECIFICATION_ROWS", trustedDfTransformation, "TRUSTED_CLEAN_DF") ::
    WriteDf("RW_ERROR_DF", rawDb, "RW_DATA_SOURCE", isTableName = false,
      SaveMode.Append, Some(ColumnName.DtBusinessDate :: Nil), impalaJdbcConnection) :: Nil
}
