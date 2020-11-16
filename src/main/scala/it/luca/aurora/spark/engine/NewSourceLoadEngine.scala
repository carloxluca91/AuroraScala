package it.luca.aurora.spark.engine

import java.time.LocalDate

import it.luca.aurora.option.Branch
import it.luca.aurora.option.ScoptParser.SourceLoadConfig
import it.luca.aurora.spark.data.NewSpecificationRecord
import it.luca.aurora.spark.exception.{MultipleSrcOrDstException, NoSpecificationException, UnexistingLookupException}
import it.luca.aurora.spark.functions.common.ColumnExpressionParser
import it.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow}
import it.luca.aurora.utils.{ColumnName, DateFormat}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

class NewSourceLoadEngine(private final val jobPropertiesFile: String)
  extends AbstractEngine(jobPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  private final var rawDfPlusTrustedColumnsOpt: Option[DataFrame] = None

  private final val writeTrustedActualTable: SourceLoadConfig => Unit = sourceLoadConfig => {

    val bancllName: String = sourceLoadConfig.bancllName
    val dtRiferimentoOpt: Option[String] = sourceLoadConfig.dtRiferimentoOpt
    val versionNumberOpt: Option[String] = sourceLoadConfig.versionNumberOpt
    val createSourceLoadLogRecord = createLogRecord(Branch.SourceLoad.name, Some(bancllName), dtRiferimentoOpt, _: String, _: String, _: Option[String])

    // Check if provided bancllName is defined
    val specifications: Seq[NewSpecificationRecord] = getSpecificationRecords(bancllName, versionNumberOpt)
    if (specifications.nonEmpty) {

      logger.info(s"Identified ${specifications.length} row(s) related to BANCLL '$bancllName'")

      val srcTables: Seq[String] = specifications.map(_.sorgenteRd).distinct
      val dstTables: Seq[String] = specifications.map(_.tabellaTd).distinct

      // Check for a unique raw table and a unique trusted table
      if (srcTables.length == 1 && dstTables.length == 1) {

        // Read data from actual table or from historical according to provided dtRferimento
        val rawActualTable: String = srcTables.head.toLowerCase
        val rawHistoricalTable: String = s"${rawActualTable}_h"
        val rawDf: DataFrame = getRawDataFrame(rawActualTable, rawHistoricalTable, dtRiferimentoOpt)

        // Added retrieved trusted columns plus a couple of technical ones
        val trustedColumns: Seq[(String, Column)] = getTrustedColumns(specifications)
        val rawDfPlusTrustedColumns: DataFrame = trustedColumns
          .foldLeft(rawDf)((df, tuple2) => df.withColumn(tuple2._1, tuple2._2))
          .withColumn(ColumnName.TsInserimento.name, lit(getJavaSQLTimestampFromNow))
          .withColumn(ColumnName.DtInserimento.name, lit(getJavaSQLDateFromNow))
          .persist()

        logger.info(s"Successfully persisted original dataframe enriched with trusted layer columns")
        rawDfPlusTrustedColumnsOpt = Some(rawDfPlusTrustedColumns)

        val trdActualTable: String = dstTables.head.toLowerCase
        writeToJDBCAndLog[Seq[NewSpecificationRecord]](pcAuroraDBName,
          trdActualTable,
          SaveMode.Overwrite,
          truncateFlag = true,
          createSourceLoadLogRecord,
          (specs: Seq[NewSpecificationRecord]) => {

            rawDfPlusTrustedColumns
              .filter(NewSpecificationRecord.reducedErrorCondition(specs))
              .select(NewSpecificationRecord.trustedDfColumns(specs): _*)
          }, specifications)

      } else throw MultipleSrcOrDstException(bancllName, srcTables, dstTables)

    } else throw NoSpecificationException(bancllName)
  }

  private final val getRawDataFrame: (String, String, Option[String]) => DataFrame =
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

    // Write trusted actual data first
    writeTrustedActualTable(sourceLoadConfig)

  }

  private def getSpecificationRecords(bancllName: String, versionNumberOpt: Option[String]): Seq[NewSpecificationRecord] = {

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
    val tableName: String = tableNameAndFilterColumn._1
    val filterConditionCol: Column = tableNameAndFilterColumn._2
    val columnsToSelect: Seq[String] = NewSpecificationRecord.columnsToSelect
    val specificationDf: DataFrame = readFromJDBC(pcAuroraDBName, tableName)
      .filter(filterConditionCol)
      .selectExpr(columnsToSelect: _*)

    // Rename each column such that, e.g. sorgente_rd => sorgenteRd, tabella_td => tabellaTd, and so on
    val regex = "_([a-z]|[A-Z])".r
    val specificationRecords: Seq[NewSpecificationRecord] = columnsToSelect
      .map(x => (x, regex.replaceAllIn(x, m => m.group(1).toUpperCase)))
      .foldLeft(specificationDf)((df, tuple2) => df.withColumnRenamed(tuple2._1, tuple2._2))
      .as[NewSpecificationRecord]
      .collect()
      .toSeq

    logger.info(f"Successfully parsed dataframe as a set of elements of type ${NewSpecificationRecord.getClass.getSimpleName}")
    specificationRecords
  }

  private def getTrustedColumns(specifications: Seq[NewSpecificationRecord]): Seq[(String, Column)] = {

    // Extract infos about lookup operations in order to filter the lookup table with proper condition
    lazy val lookupTypesAndIDs: Seq[(String, String)] = specifications
      .filter(_.isLookupFlagOn)
      .map(x => (x.tipoLookup.get.toLowerCase, x.lookupId.get.toLowerCase))

    lazy val lookupDfFilterConditionCol: Column = lookupTypesAndIDs
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

    specifications
      .sortBy(_.posizioneFinale)
      .map(s => {

        logger.info(s"Analyzing specifications related to trusted column '${s.colonnaTd}'")
        val trustedColumnBeforeLookup: Column = s.funzioneEtl match {
          case None =>

            // If an ETL expression has not been defined, get simple raw column
            logger.info(s"No ETL function to apply to input column '${s.colonnaRd.get}'")
            col(s.colonnaRd.get)

          case Some(etlFunction) =>

            // Otherwise, parse provided ETL function
            logger.info(s"Detected an ETL function to apply: '$etlFunction'")
            val etlTransformedColumn: Column = ColumnExpressionParser(etlFunction)
            logger.info(s"Successfully parsed ETL function '$etlFunction")
            etlTransformedColumn

        }

        // If a lookup operation is specified
        val trustedColumnAfterLookup: Column = if (s.isLookupFlagOn) {

          // Try to retrieve related cases
          val lookupType = s.tipoLookup.get.toLowerCase()
          val lookupId = s.tipoLookup.get.toLowerCase
          val lookupDfFilterCondition: Column = trim(lower(col(ColumnName.LookupTipo.name))) === lookupType &&
            trim(lower(col(ColumnName.LookupId.name))) === lookupId

          val lookupCases: Seq[Row] = lookupDf
            .filter(lookupDfFilterCondition)
            .select(ColumnName.LookupValoreOriginale.name, ColumnName.LookupValoreSostituzione.name)
            .collect()
            .toSeq

          if (lookupCases.nonEmpty) {

            // If some cases are retrieved, prepare a suitable case-when statement
            logger.info(s"Identified ${lookupCases.size} case(s) for lookup type '$lookupType', id '$lookupId'")
            val firstCaseColumn: Column = when(trustedColumnBeforeLookup === lookupCases.head.get(0), lookupCases.head.get(1))
            lookupCases.tail
              .foldLeft(firstCaseColumn)((column, row) =>
              column.when(trustedColumnBeforeLookup === row.get(0), row.get(1)))

          } else throw UnexistingLookupException(s.colonnaTd, lookupType, lookupId)

          // Otherwise, get simple column expression after ETL
        } else trustedColumnBeforeLookup

        (s.colonnaTd, trustedColumnAfterLookup)
      })
  }
}
