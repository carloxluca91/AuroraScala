package it.luca.aurora.spark.engine

import java.time.LocalDate

import it.luca.aurora.option.Branch
import it.luca.aurora.option.ScoptParser.SourceLoadConfig
import it.luca.aurora.spark.data.{LogRecord, SpecificationRecord, Specifications}
import it.luca.aurora.exception.NoSpecificationException
import it.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow, insertElementAtIndex}
import it.luca.aurora.utils.{ColumnName, DateFormat}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

case class SourceLoadEngine(override val jobPropertiesFile: String)
  extends AbstractEngine(jobPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final var specificationsOpt: Option[Specifications] = None
  private final var trdActualTableNameOpt: Option[String] = None
  private final var rawDfPlusTrustedColumnsOpt: Option[DataFrame] = None

  private final val getTrdActualTable: SourceLoadConfig => DataFrame = sourceLoadConfig => {

    val bancllName: String = sourceLoadConfig.bancllName
    val dtRiferimentoOpt: Option[String] = sourceLoadConfig.dtRiferimentoOpt
    val versionNumberOpt: Option[String] = sourceLoadConfig.versionNumberOpt

    // Check if provided bancllName is defined
    val specifications: Specifications = getSpecifications(bancllName, versionNumberOpt)
    if (specifications.nonEmpty) {

      // Store retrieved specifications (for next writing operations)
      logger.info(s"Identified ${specifications.length} row(s) related to BANCLL '$bancllName'")
      specificationsOpt = Some(specifications)
      trdActualTableNameOpt = Some(specifications.trdActualTableName.toLowerCase)

      // Read data from actual table or from historical according to provided dtRiferimento
      val rawActualTable: String = specifications.rwdActualTableName.toLowerCase
      val rawHistoricalTable: String = s"${rawActualTable}_h"
      val rawDf: DataFrame = getDfToBeIngested(rawActualTable, rawHistoricalTable, dtRiferimentoOpt)

      val primaryKeyColumns: Seq[Column] = specifications.primaryKeyColumns
      val trustedColumns: Seq[(String, Column)] = getTrdColumns(specifications)

      // Added retrieved trusted columns plus a triple of technical ones
      val rawDfPlusTrustedColumns: DataFrame = trustedColumns
        .foldLeft(rawDf)((df, tuple2) => df.withColumn(tuple2._1, tuple2._2))
        .withColumn(ColumnName.RowCount.name, count("*") over Window.partitionBy(primaryKeyColumns: _*))
        .withColumn(ColumnName.TsInserimento.name, lit(getJavaSQLTimestampFromNow))
        .withColumn(ColumnName.DtInserimento.name, lit(getJavaSQLDateFromNow))
        .persist()

      // Store the dataframe obtained so far (for next writing operations)
      logger.info(s"Successfully persisted original dataframe enriched with trusted layer columns")
      rawDfPlusTrustedColumnsOpt = Some(rawDfPlusTrustedColumns)
      getTrdCleanDataDf(s => s.trdDfColumnSet)

    } else throw NoSpecificationException(bancllName)
  }

  private final val getDfToBeIngested: (String, String, Option[String]) => DataFrame =
    (actualTable, historicalTable, dtRiferimentoOpt) => {

      dtRiferimentoOpt match {
        case None =>

          // Read from actual table with no filtering
          logger.info(s"No business date has been provided. Thus, reading raw data from '$actualTable'")
          readFromJDBC(lakeCedacriDBName, actualTable)

        case Some(value) =>

          // Read from historical table providing proper filtering
          logger.info(s"Provided dt_riferimento: '$value'. Thus, reading raw data from '$historicalTable'")
          val dtRiferimentoSQLDate: java.sql.Date =  java.sql.Date.valueOf(LocalDate.parse(value, DateFormat.DtRiferimento.formatter))
          readFromJDBC(lakeCedacriDBName, historicalTable)
          .filter(col(ColumnName.DtRiferimento.name) === dtRiferimentoSQLDate)
      }
  }

  private final val getTrdCleanDataDf: (Specifications => Seq[Column]) => DataFrame = selectOperation => {

    val specifications = specificationsOpt.get
    val rawDfPlusTrustedColumns = rawDfPlusTrustedColumnsOpt.get

    val cleanDataFilterCondition: Column = (!specifications.errorCondition) && col(ColumnName.RowCount.name) === 1
    rawDfPlusTrustedColumns
      .filter(cleanDataFilterCondition)
      .select(selectOperation(specifications): _*)
  }

  private final val createErrorDescriptionCol: UserDefinedFunction =
    udf((s: Seq[Option[String]]) => {

      val distinctSeq: Seq[String] = s
        .filter(_.nonEmpty)
        .map(_.get)
        .map(_.split(", "))
        .flatMap(_.toList)
        .distinct

      if (distinctSeq.nonEmpty) {

        Some(s"${distinctSeq.size} invalid column(s): " + distinctSeq.mkString(", "))
      } else None
    })

  def run(sourceLoadConfig: SourceLoadConfig): Unit = {

    val bancllName: String = sourceLoadConfig.bancllName
    val dtRiferimentoOpt: Option[String] = sourceLoadConfig.dtRiferimentoOpt
    val createSourceLoadLogRecord = LogRecord(sparkSession.sparkContext, Branch.SourceLoad.name, Some(bancllName), dtRiferimentoOpt,
      _: String, _: String, _: Option[String])

    // Write actual trd table
    writeToJDBCAndLog[SourceLoadConfig](pcAuroraDBName,
      trdActualTableNameOpt.get,
      SaveMode.Overwrite,
      truncateFlag = true,
      createSourceLoadLogRecord,
      getTrdActualTable,
      sourceLoadConfig)

    if ((specificationsOpt :: rawDfPlusTrustedColumnsOpt :: Nil).forall(_.nonEmpty)) {

      // Write historical trd table
      val specifications = specificationsOpt.get
      val trdHistoricalTable = s"${specifications.trdActualTableName}_h"
      writeToJDBCAndLog[Specifications => Seq[Column]](pcAuroraDBName,
        trdHistoricalTable,
        SaveMode.Append,
        truncateFlag = true,
        createSourceLoadLogRecord,
        getTrdCleanDataDf,
        s => s.trdDfColumnSet)

      // Write rwd layer error table(s)
      val rwdErrorActualTable = s"${specifications.rwdActualTableName}_error"
      val rwdErrorHistoricalTable = s"${rwdErrorActualTable}_h"
      logger.info(s"Starting to populate rwd layer error tables ($rwdErrorActualTable, $rwdErrorHistoricalTable) on db $lakeCedacriDBName")
      ((rwdErrorActualTable -> SaveMode.Overwrite) :: (rwdErrorHistoricalTable -> SaveMode.Append) :: Nil) foreach {
        t => val (tableName, saveMode): (String, SaveMode) = t
          writeToJDBCAndLog[Specifications](lakeCedacriDBName,
            tableName,
            saveMode,
            truncateFlag = true,
            createSourceLoadLogRecord,
            getRwdErrorDf,
            specifications)
      }

      logger.info(s"Successfully populated rwd layer error tables ($rwdErrorActualTable, $rwdErrorHistoricalTable) on db $lakeCedacriDBName")

      // Write trd layer duplicates table
      val trdDuplicatesActualTable = s"${specifications.trdActualTableName}_duplicated"
      val trdDuplicatesHistoricalTable = s"${trdDuplicatesActualTable}_h"
      logger.info(s"Starting to populate trd layer duplicated records tables ($trdDuplicatesActualTable, $trdDuplicatesHistoricalTable) " +
        s"on db $pcAuroraDBName")
      ((trdDuplicatesActualTable -> SaveMode.Overwrite) :: (trdDuplicatesHistoricalTable -> SaveMode.Append) :: Nil) foreach {
        t => val (tableName, saveMode): (String, SaveMode) = t
          writeToJDBCAndLog[Specifications](pcAuroraDBName,
            tableName,
            saveMode,
            truncateFlag = true,
            createSourceLoadLogRecord,
            getTrdDuplicatedDf,
            specifications)
      }

      logger.info(s"Successfully populated trd layer duplicated records tables ($trdDuplicatesActualTable, $trdDuplicatesHistoricalTable) " +
        s"on db $pcAuroraDBName")

    } else {
      logger.warn(s"Skipping writing of next tables due to some error on previous step")
    }
  }

  private def getRwdErrorDf(specifications: Specifications): DataFrame = {

    val rawDfPlusTrustedColumns: DataFrame = rawDfPlusTrustedColumnsOpt.get

    // Retrieve definition of columns containing error description
    val errorDescriptions: Seq[(String, Column)] = specifications.errorDescriptions
    val errorDescriptionColumnNames: Seq[String] = errorDescriptions.map(_._1)
    val rawDfPlusTrustedColumnsAndErrorDescriptions: DataFrame = errorDescriptions
      .foldLeft(rawDfPlusTrustedColumns)((df, tuple) => {
        df.withColumn(tuple._1, tuple._2)
      })
      .withColumn(ColumnName.ErrorDescription.name,
        createErrorDescriptionCol(array(errorDescriptionColumnNames.head, errorDescriptionColumnNames.tail: _*)))

    logger.info(s"Successfully added error description columns")
    val rwdDfColumns: Seq[Column] = specifications.rwdDfColumnSet
    val rwdDfColumnsPlusErrorDescription: Seq[Column] = insertElementAtIndex(specifications.rwdDfColumnSet,
      col(ColumnName.ErrorDescription.name), rwdDfColumns.indexOf(col(ColumnName.TsInserimento.name)))

    rawDfPlusTrustedColumnsAndErrorDescriptions
      .filter(specifications.errorCondition)
      .select(rwdDfColumnsPlusErrorDescription: _*)
  }

  private def getTrdDuplicatedDf(specifications: Specifications): DataFrame = {

    val rawDfPlusTrustedColumns: DataFrame = rawDfPlusTrustedColumnsOpt.get
    rawDfPlusTrustedColumns
      .filter(specifications.errorCondition && col(ColumnName.RowCount.name) =!= 1)
      .select(specifications.trdDfColumnSet: _*)
  }

  private def getSpecifications(bancllName: String, versionNumberOpt: Option[String]): Specifications = {

    import sparkSession.implicits._

    val tableNameAndFilterColumn: (String, Column) = versionNumberOpt match {
      case None =>

        // If no version number has been provided, read specification from actual table
        logger.info(s"No specification version number provided. Thus, reading specifications from '$pcAuroraDBName'.'$mappingSpecificationTBLName'")
        (mappingSpecificationTBLName, col(ColumnName.Flusso.name) === bancllName)

      case Some(versionNumber) =>

        val mappingSpecificationHistTBLName: String = jobProperties.getString("table.mapping_specification_historical.name")
        logger.info(s"Specification version number to be used: '$versionNumber'. " +
          s"Thus, reading specifications from '$pcAuroraDBName'.'$mappingSpecificationHistTBLName'")
        (mappingSpecificationHistTBLName, col(ColumnName.Flusso.name) === bancllName && col(ColumnName.Versione.name) === versionNumber)
    }

    // Retrieve information for given bancllName
    val (tableName, filterConditionCol): (String, Column) = tableNameAndFilterColumn
    val columnsToSelect: Seq[String] = SpecificationRecord.columnsToSelect
    val specificationDf: DataFrame = readFromJDBC(pcAuroraDBName, tableName)
      .filter(filterConditionCol)
      .selectExpr(columnsToSelect: _*)

    // Rename each column such that, e.g. sorgente_rd => sorgenteRd, tabella_td => tabellaTd, and so on
    val regex = "_([a-z]|[A-Z])".r
    val specificationRecords: Seq[SpecificationRecord] = columnsToSelect
      .map(x => (x, regex.replaceAllIn(x, m => m.group(1).toUpperCase)))
      .foldLeft(specificationDf)((df, tuple2) => df.withColumnRenamed(tuple2._1, tuple2._2))
      .as[SpecificationRecord]
      .collect()
      .toSeq

    logger.info(f"Successfully parsed dataframe as a set of elements of type ${classOf[SpecificationRecord].getSimpleName}")
    Specifications(specificationRecords)
  }

  private def getTrdColumns(specifications: Specifications): Seq[(String, Column)] = {

    // Extract infos about lookup operations in order to filter the lookup table with proper condition
    lazy val lookupDfFilterConditionCol: Column = specifications
      .lookupTypesAndIds
      .map(t => {

        val (lookupType, lookupId): (String, String) = t
        trim(lower(col(ColumnName.LookupTipo.name))) === lookupType &&
          trim(lower(col(ColumnName.LookupId.name))) === lookupId
      })
      .reduce(_ || _)

    // Read lookup table exploiting filter condition newly defined
    lazy val lookupDf: DataFrame = readFromJDBC(pcAuroraDBName, lookupTBLName)
      .filter(lookupDfFilterConditionCol)
      .persist()

    specifications.trdColumns(lookupDf)
  }
}
