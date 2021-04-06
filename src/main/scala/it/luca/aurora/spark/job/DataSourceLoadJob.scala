package it.luca.aurora.spark.job

import it.luca.aurora.enumeration.{Branch, ColumnName, DateFormat}
import it.luca.aurora.exception.{MultipleMappingsException, NoMappingException, NoSpecificationException}
import it.luca.aurora.option.DataSourceLoadConfig
import it.luca.aurora.spark.bean.{MappingRow, SpecificationRow, SpecificationRows}
import it.luca.aurora.spark.implicits._
import it.luca.aurora.spark.step._
import it.luca.aurora.utils.{now, toDate}
import org.apache.spark.sql.functions.{col, concat_ws, lit, size, split, when}
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}

case class DataSourceLoadJob(override val sqlContext: SQLContext,
                             override val propertiesFile: String,
                             private val config: DataSourceLoadConfig)
  extends SparkJob(sqlContext, propertiesFile, Branch.DataSourceLoad) {

  private val Latest = "LATEST"

  override protected val dataSource: Option[String] = Some(config.dataSource)
  override protected val dtBusinessDate: Option[String] = config.dtBusinessDate
  override protected val specificationVersion: Option[String] = Some(config.specificationVersion.getOrElse(Latest))

  // Hive properties
  private val mappingHistorical: String = jobProperties.getString("hive.table.mapping.historical")
  private val specificationHistorical: String = jobProperties.getString("hive.table.specification.historical")
  private val rawDb: String = jobProperties.getString("hive.db.raw")

  private def getHiveQlQuery(actualTable: String, historicalTable: String): String = {

    if (specificationVersion.equals(Some(Latest))) {
      s"""SELECT *
         |FROM $trustedDb.$actualTable
         |WHERE TRIM(LOWER(${ColumnName.DataSource})) = '${config.dataSource}'
         |""".stripMargin
    } else {
      s"""SELECT *
         |FROM $trustedDb.$historicalTable
         |WHERE VERSION = '$specificationVersion'
         |AND TRIM(LOWER(${ColumnName.DataSource})) = '${config.dataSource}'
         |""".stripMargin
    }
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

  override protected val steps: Seq[Step[_]] = {

    // Retrieve MappingRow for given dataSource
    (QueryAndDecode[MappingRow](getHiveQlQuery(mappingActual, mappingHistorical), sqlContext, "MAPPING_BEANS") ::
      FromTo[Seq[MappingRow], MappingRow]("MAPPING_BEANS", beans => beans.size match {
        case 0 => throw NoMappingException(config.dataSource)
        case 1 => beans.head
        case _ => throw MultipleMappingsException(beans)
      }, "MAPPING_ROW") ::

      // Retrieve SpecificationRows for given dataSource
      QueryAndDecode[SpecificationRow](getHiveQlQuery(specificationActual, specificationHistorical), sqlContext, "SPECIFICATION_BEANS") ::
      FromTo[Seq[SpecificationRow], SpecificationRows]("SPECIFICATION_BEANS", beans => beans.size match {
        case 0 => throw NoSpecificationException(config.dataSource)
        case _ => SpecificationRows.from(beans)
      }, "SPECIFICATION_ROWS") ::

      // Read input raw-layer data and transform them
      ReadRawData("MAPPING_ROW", config.dtBusinessDate.getOrElse(toDate(now(), DateFormat.DateDefault)), sqlContext, "RW_DF") ::
      TransformDfWrtSpecifications("RW_DF", "SPECIFICATION_ROWS", errorDfTransformation, "RW_ERROR_DF") ::
      TransformDfWrtSpecifications("RW_DF", "SPECIFICATION_ROWS", trustedDfTransformation, "TRUSTED_CLEAN_DF") ::
      WriteDf("RW_ERROR_DF", rawDb, "RW_DATA_SOURCE", isTableName = false,
        SaveMode.Append, Some(ColumnName.DtBusinessDate :: Nil), impalaJdbcConnection) :: Nil)
  }
}
