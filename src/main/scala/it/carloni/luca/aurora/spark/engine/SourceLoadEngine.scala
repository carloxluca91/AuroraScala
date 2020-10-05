package it.carloni.luca.aurora.spark.engine

import java.time.LocalDate

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.option.ScoptParser.SourceLoadConfig
import it.carloni.luca.aurora.spark.data.{LogRecord, SpecificationRecord}
import it.carloni.luca.aurora.spark.exception.{MultipleSrcOrDstException, NoSpecificationException}
import it.carloni.luca.aurora.spark.functions.constant.ConstantFunctionFactory
import it.carloni.luca.aurora.spark.functions.etl.ETLFunctionFactory
import it.carloni.luca.aurora.utils.Utils._
import it.carloni.luca.aurora.utils.{ColumnName, DateFormat}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

import scala.util.matching.Regex

class SourceLoadEngine(val jobPropertiesFile: String)
  extends AbstractEngine(jobPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  private final val createNullColumnsDescription: UserDefinedFunction =
    udf((columnNames: Seq[String], columnValues: Seq[String]) => {

    columnNames.zip(columnValues)
      .filter(x => x._2 == null)
      .map(x => s"${x._1} (null)")
      .mkString(", ")
  })

  private final val createNotNullColumnsDescription: UserDefinedFunction =
    udf((columnNames: Seq[String], columnValues: Seq[String]) => {

    columnNames.zip(columnValues)
      .map(x => s"${x._1} (${x._2})")
      .mkString(", ")
  })

  private final val createErrorDescriptionCol: UserDefinedFunction =
    udf((s: Seq[String]) => {

    val distinctSeq: Seq[String] = s
      .filter(_ != null)
      .map(_.split(", "))
      .flatMap(_.toList)
      .distinct

    if (distinctSeq.nonEmpty) {

      Some(s"${distinctSeq.size} invalid column(s): " + distinctSeq.mkString(", "))
    } else None
  })

  private var rawDfPlusTrustedColumnsOpt: Option[DataFrame] = None

  def run(sourceLoadConfig: SourceLoadConfig): Unit = {

    val bancllName: String = sourceLoadConfig.bancllName
    val dtRiferimentoOpt: Option[String] = sourceLoadConfig.dtRiferimentoOpt
    val versionNumberOpt: Option[Double] = sourceLoadConfig.versionNumberOpt
    val createSourceLoadLogRecord = createLogRecord(Branch.SOURCE_LOAD.getName,
      Some(bancllName),
      dtRiferimentoOpt,
      _: String,
      _: String,
      _: Option[String])

    // CHECK IF CURRENT BANCLL IS DEFINED
    val specificationRecords: Seq[SpecificationRecord] = getSpecificationRecords(bancllName, versionNumberOpt)
    if (specificationRecords.nonEmpty) {

      logger.info(s"Identified ${specificationRecords.length} row(s) related to BANCLL '$bancllName'")

      // CHECK THAT ONLY 1 RAW TABLE AND ONLY 1 TRUSTED TABLE HAVE BEEN SPECIFIED FOR THIS BANCLL
      val srcTables: Seq[String] = specificationRecords.map(_.sorgenteRd).distinct
      val dstTables: Seq[String] = specificationRecords.map(_.tabellaTd).distinct

      if (srcTables.length == 1 && dstTables.length == 1) {

        val rwActualTableName: String = srcTables.head.toLowerCase
        val rwHistoricalTableName: String = rwActualTableName + "_h"
        val trdActualTableName: String = dstTables.head.toLowerCase
        val trdHistoricalTableName: String = trdActualTableName + "_h"

        logger.info(s"BANCLL '$bancllName' -> Raw actual table: '$rwActualTableName', Raw historical table: '$rwHistoricalTableName'")
        logger.info(s"BANCLL '$bancllName' -> Trusted actual table: '$trdActualTableName', Trusted historical table: '$trdHistoricalTableName'")

        // TRY TO OVERWRITE TRD ACTUAL TABLE WITH CLEAN DATA
        writeToJDBCAndLog[Seq[SpecificationRecord]](
          pcAuroraDBName,
          trdActualTableName,
          SaveMode.Overwrite,
          truncateFlag = true,
          createSourceLoadLogRecord,
          (specifications: Seq[SpecificationRecord]) => {

            val rawDf: DataFrame = getRawDataFrame(rwActualTableName, rwHistoricalTableName, dtRiferimentoOpt)
            persistRawDfPlusTrustedColumns(rawDf, specifications)
            rawDfPlusTrustedColumnsOpt.get
              .filter(!getErrorFilterConditionCol(specificationRecords))
              .select(getColsToSelect(specificationRecords,
                None,
                s => s.posizioneFinale,
                s => col(s.colonnaTd)): _*)},
          specificationRecords)

        if (rawDfPlusTrustedColumnsOpt.nonEmpty) {

          // TRY TO APPEND CLEAN DATA ON TRD HISTORICAL TABLE
          writeToJDBCAndLog[Seq[SpecificationRecord]](
            pcAuroraDBName,
            trdHistoricalTableName,
            SaveMode.Append,
            truncateFlag = false,
            createSourceLoadLogRecord,
            (specifications: Seq[SpecificationRecord]) => {

              rawDfPlusTrustedColumnsOpt.get
                .filter(!getErrorFilterConditionCol(specifications))
                .select(getColsToSelect(specificationRecords,
                  None,
                  s => s.posizioneFinale,
                  s => col(s.colonnaTd)): _*)},
            specificationRecords)

        } else {

          logger.warn(s"Skipping insert operation for table(s) '$pcAuroraDBName'.'$trdHistoricalTableName'")
        }

        // WRITE OTHER RELATED TABLES
        writeErrorAndDuplicatedTables(specificationRecords, rwActualTableName, trdActualTableName, createSourceLoadLogRecord)

      } else throw new MultipleSrcOrDstException(bancllName, srcTables, dstTables)

    } else throw new NoSpecificationException(bancllName)
  }

  private def getSpecificationRecords(bancllName: String, versionNumberOpt: Option[Double]): Seq[SpecificationRecord] = {

    import sparkSession.implicits._

    logger.info(s"Provided BANCLL name: '$bancllName'")
    val mappingSpecificationTBLNameAndFilterColumn: (String, Column) = if (versionNumberOpt.isEmpty) {

      // NO VERSION NUMBER PROVIDED => READ ACTUAL TABLE SPECIFICATION
      logger.info(s"No specification version number provided. Thus, reading specifications from '$pcAuroraDBName'.'$mappingSpecificationTBLName'")
      (mappingSpecificationTBLName, col(ColumnName.FLUSSO.getName) === bancllName)

    } else {

      // VERSION NUMBER PROVIDED => READ HISTORICAL SPECIFICATION TABLE
      val versionNumber: Double = versionNumberOpt.get
      val versionNumberFormatted: String = f"$versionNumber%.1f"
        .replace(',', '.')

      val mappingSpecificationHistTBLName: String = jobProperties.getString("table.mapping_specification_historical.name")
      logger.info(s"Specification version number to be used: '$versionNumberFormatted'. Thus, reading specifications from '$pcAuroraDBName'.'$mappingSpecificationHistTBLName'")
      (mappingSpecificationHistTBLName, col(ColumnName.FLUSSO.getName) === bancllName && col(ColumnName.VERSIONE.getName) === versionNumber)
    }

    // RETRIEVE SPECIFICATIONS FOR GIVEN bancllName
    val mappingSpecificationTableToRead: String = mappingSpecificationTBLNameAndFilterColumn._1
    val mappingSpecificationFilterCol: Column = mappingSpecificationTBLNameAndFilterColumn._2

    val toSelect: Seq[String] = Seq("flusso", "sorgente_rd", "tabella_td", "colonna_rd", "tipo_colonna_rd", "flag_discard",
      "posizione_iniziale", "funzione_etl", "flag_lookup", "tipo_lookup", "lookup_id", "colonna_td",
      "tipo_colonna_td", "posizione_finale", "flag_primary_key")

    val specificationDf: DataFrame = readFromJDBC(pcAuroraDBName, mappingSpecificationTableToRead)
      .filter(mappingSpecificationFilterCol)
      .selectExpr(toSelect: _*)

    // RENAME EACH COLUMN WITH A RULE SUCH THAT, e.g. sorgente_rd => sorgenteRd, tabella_td => tabellaTd
    val regex: Regex = "_([a-z]|[A-Z])".r
    val specificationRecords: Seq[SpecificationRecord] = toSelect
      .map(x => (x, regex.replaceAllIn(x, m => m.group(1).toUpperCase)))
      .foldLeft(specificationDf)((df, tuple2) => df.withColumnRenamed(tuple2._1, tuple2._2))
      .as[SpecificationRecord]
      .collect()
      .toSeq

    logger.info(f"Successfully parsed dataframe as a set of elements of type ${SpecificationRecord.getClass.getSimpleName}")
    specificationRecords
  }

  private def getRawDataFrame(rawActualTableName: String, rawHistoricalTableName: String, dtRiferimentoOpt: Option[String]): DataFrame = {

    if (dtRiferimentoOpt.nonEmpty) {

      // IF A dt_riferimento HAS BEEN PROVIDED, READ FROM RAW_HISTORICAL_TABLE
      val dtRiferimentoStr: String = dtRiferimentoOpt.get
      logger.info(s"Provided business date: '$dtRiferimentoStr'. Thus, reading raw data from '$rawHistoricalTableName'")

      val dtRiferimentoSQLDate: java.sql.Date =  java.sql.Date.valueOf(LocalDate.parse(dtRiferimentoStr, DateFormat.DT_RIFERIMENTO.getFormatter))
      readFromJDBC(lakeCedacriDBName, rawHistoricalTableName)
        .filter(col(ColumnName.DT_RIFERIMENTO.getName) === dtRiferimentoSQLDate)

    } else {

      // OTHERWISE, READ FROM RAW_ACTUAL_TABLE
      logger.info(s"No business date has been provided. Thus, reading raw data from '$rawActualTableName'")
      readFromJDBC(lakeCedacriDBName, rawActualTableName)
    }
  }

  private def writeErrorAndDuplicatedTables(specificationRecords: Seq[SpecificationRecord],
                                            rwActualTableName: String,
                                            trdActualTableName: String,
                                            createLogRecord: (String, String, Option[String]) => LogRecord): Unit = {

    val tablesToWrite: Map[(String, String), Seq[SpecificationRecord] => DataFrame] = Map(

      // RW_ERROR
      (lakeCedacriDBName, rwActualTableName.concat("_error")) -> (getErrorDf(_,
        Some({s => s.colonnaRd.nonEmpty}),
        s => s.posizioneIniziale.get,
        s => col(s.colonnaRd.get))),

      // TRD_ERROR
      (pcAuroraDBName, trdActualTableName.concat("_error")) -> (getErrorDf(_,
        None,
        s => s.posizioneFinale,
        s => col(s.colonnaTd))),

      // TRD_DUPLICATED
      (pcAuroraDBName, trdActualTableName.concat("_duplicated")) -> getDuplicatesRecordDf)

    // FOR EACH (k, v) PAIR
    tablesToWrite
      .foreach(x => {

        // UNWRAP INFORMATION AND FUNCTION
        val db: String = x._1._1
        val actualTable: String = x._1._2
        val historicalTable: String = actualTable + "_h"
        val dfGenerationFunction: Seq[SpecificationRecord] => DataFrame = x._2

        if (rawDfPlusTrustedColumnsOpt.isEmpty) {

          logger.warn(s"Skipping insert operation for table(s) '$db'.'$actualTable', '$db'.'$historicalTable'")

        } else {

          // OVERWRITE ACTUAL TABLE
          writeToJDBCAndLog[Seq[SpecificationRecord]](
            db,
            actualTable,
            SaveMode.Overwrite,
            truncateFlag = true,
            createLogRecord,
            dfGenerationFunction,
            specificationRecords)

          // APPEND DATA ON HISTORICAL TABLE
          writeToJDBCAndLog[Seq[SpecificationRecord]](
            db,
            historicalTable,
            SaveMode.Append,
            truncateFlag = false,
            createLogRecord,
            dfGenerationFunction,
            specificationRecords)
        }
      })
  }

  private def getColsToSelect(specificationRecords: Seq[SpecificationRecord],
                              specificationRecordFilterOp: Option[SpecificationRecord => Boolean],
                              specificationRecordSortingOp: SpecificationRecord => Int,
                              specificationRecordToColumnOp: SpecificationRecord => Column): Seq[Column] = {

    val specificationRecordsMaybeFiltered: Seq[SpecificationRecord] = if (specificationRecordFilterOp.isEmpty) {

      specificationRecords
    } else {

      specificationRecords
        .filter(specificationRecordFilterOp.get)
    }

    // DEPENDING ON THE PROVIDED op, DEFINES SET OF RW OR TRUSTED COLUMNS TO SELECT
    val columnsSorted: Seq[Column] = specificationRecordsMaybeFiltered
      .sortBy(specificationRecordSortingOp)
      .map(specificationRecordToColumnOp)

    (col(ColumnName.ROW_ID.getName) +: columnsSorted) ++ Seq(col(ColumnName.TS_INSERIMENTO.getName),
      col(ColumnName.DT_INSERIMENTO.getName),
      col(ColumnName.DT_RIFERIMENTO.getName))
  }

  private def persistRawDfPlusTrustedColumns(rawDf: DataFrame, specificationRecords: Seq[SpecificationRecord]): Unit = {

    // ENRICH RAW DATAFRAME WITH COLUMNS DERIVED FROM SPECIFICATIONS
    val trustedColumns: Seq[(Column, String)] = defineTrustedColumns(specificationRecords)
    val rawDfPlusTrustedColumns: DataFrame = trustedColumns
      .foldLeft(rawDf)((df, tuple2) => df.withColumn(tuple2._2, tuple2._1))

      // TECHNICAL COLUMNS
      .withColumn(ColumnName.TS_INSERIMENTO.getName, lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DT_INSERIMENTO.getName, lit(getJavaSQLDateFromNow))
      .persist()

    rawDfPlusTrustedColumnsOpt = Some(rawDfPlusTrustedColumns)
  }

  private def defineTrustedColumns(specificationRecords: Seq[SpecificationRecord]): Seq[(Column, String)] = {

    val lookUpTypesAndIds: Seq[(String, String)] = specificationRecords
      .filter(_.flagLookup.nonEmpty)
      .filter(_.flagLookup.get equalsIgnoreCase "y")
      .map(x => (x.tipoLookup.get.toLowerCase, x.lookupId.get.toLowerCase))
      .distinct

    logger.info(s"Identified ${lookUpTypesAndIds.size} lookup-related tuple(s) (${lookUpTypesAndIds.map(x => s"category = '${x._1}', id = '${x._2}'")})")

    val lookUpDataFrameFilterConditionCol: Column = lookUpTypesAndIds
      .map(x => lower(col(ColumnName.LOOKUP_TIPO.getName)) === x._1 &&
        lower(col(ColumnName.LOOKUP_ID.getName)) === x._2)
      .reduce(_ || _)

    lazy val lookUpDataframe: DataFrame = readFromJDBC(pcAuroraDBName, lookupTBLName)
      .filter(lookUpDataFrameFilterConditionCol)
      .persist()

    val trustedColumnsDerivedFromRwColumns: Seq[(Int, Column, String)] = specificationRecords
      .filter(_.colonnaRd.nonEmpty)
      .map(specificationRecord => {

        val rwColumnName: String = specificationRecord.colonnaRd.get
        val rwColumn: Column = col(rwColumnName)

        logger.info(s"Analyzing specification for raw column '$rwColumnName'")
        val trustedColumnBeforeLK: Column = if (!specificationRecord.involvesATransformation) {

          // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT NEEDS TO BE KEPT
          logger.info(s"No transformation to apply to raw column '$rwColumnName'")
          rwColumn

        } else {

          // OTHERWISE, THE COLUMN IMPLIES SOME TRANSFORMATION
          ETLFunctionFactory(specificationRecord.funzioneEtl.get, rwColumn)
        }

        // CHECK LOOKUP FLAG
        val flagLookUp: Boolean = if (!specificationRecord.involvesLookUp) false else {

          val flagLookUpStr: String = specificationRecord.flagLookup.get
          logger.info(f"Lookup flag for column '${specificationRecord.colonnaRd.get}': '$flagLookUpStr''")
          flagLookUpStr equalsIgnoreCase "y"
        }

        // IF true, DEFINE A CASE-WHEN COLUMN ABLE TO CATCH PROVIDED CASES
        val trustedColumnAfterLK: Column = if (flagLookUp) {

          val tipoLookUp: String = specificationRecord.tipoLookup.get.toLowerCase
          val lookupId: String = specificationRecord.lookupId.get.toLowerCase
          val lookUpCaseRows: Seq[Row] = lookUpDataframe
            .filter(lower(col(ColumnName.LOOKUP_TIPO.getName)) === tipoLookUp && lower(col(ColumnName.LOOKUP_ID.getName)) === lookupId)
            .select(ColumnName.LOOKUP_VALORE_ORIGINALE.getName, ColumnName.LOOKUP_VALORE_SOSTITUZIONE.getName)
            .collect()

          logger.info(s"Identified ${lookUpCaseRows.size} case(s) for lookup type '$tipoLookUp', id '$lookupId'")

          val firstLookUpCase: Row = lookUpCaseRows.head
          val foldLeftSeedCol: Column = when(trustedColumnBeforeLK === firstLookUpCase.get(0), firstLookUpCase.get(1))
          lookUpCaseRows.tail
            .foldLeft(foldLeftSeedCol)((col1, row) =>
              col1.when(trustedColumnBeforeLK === row.get(0), row.get(1)))

        } else trustedColumnBeforeLK

        (specificationRecord.posizioneFinale, trustedColumnAfterLK, specificationRecord.colonnaTd)
      })

    val newlyTrustedColumns: Seq[(Int, Column, String)] = specificationRecords
      .filter(x => x.colonnaRd.isEmpty & x.funzioneEtl.nonEmpty)
      .map(x => {

        (x.posizioneFinale, ConstantFunctionFactory(x.funzioneEtl.get), x.colonnaTd)
      })

    (trustedColumnsDerivedFromRwColumns ++ newlyTrustedColumns)
      .sortBy(_._1)
      .map(x => (x._2, x._3))
  }

  private def getErrorDf(specifications: Seq[SpecificationRecord],
                         specificationRecordFilterOp: Option[SpecificationRecord => Boolean],
                         specificationRecordSortingOp: SpecificationRecord => Int,
                         specificationRecordToColumnOp: SpecificationRecord => Column): DataFrame = {

    val tdErrorDescriptionColumns: Seq[(String, Column)] = specifications
      .filter(x => x.colonnaRd.nonEmpty)
      .map(x => {

        val (areOtherRwColumnsInvolved, rwInvolvedColumnNamesOpt): (Boolean, Option[Seq[String]]) = x.involvesOtherRwColumns
        val allOfRwColumnsInvolved: Seq[String] = if (areOtherRwColumnsInvolved) {

          x.colonnaRd.get +: rwInvolvedColumnNamesOpt.get
        } else x.colonnaRd.get :: Nil

        (x.colonnaTd + "_error", when(x.areSomeRwColumnsNull,
          createNullColumnsDescription(array(allOfRwColumnsInvolved.map(lit): _*), array(allOfRwColumnsInvolved.map(col): _*)))
          .when(x.isTrdColumnNullWhileRwColumnsAreNot,
            createNotNullColumnsDescription(array(allOfRwColumnsInvolved.map(lit): _*), array(allOfRwColumnsInvolved.map(col): _*))))
      })

    // GET INITIAL SET OF COLUMNS AND THEN ADD ERROR DESCRIPTION COLUMN (BEFORE 'ts_inserimento')
    val columnsToSelect: Seq[Column] = getColsToSelect(specifications,
      specificationRecordFilterOp,
      specificationRecordSortingOp,
      specificationRecordToColumnOp)

    val columnsToSelectPlusErrorDescription: Seq[Column] = insertElementAtIndex(columnsToSelect,
      col(ColumnName.ERROR_DESCRIPTION.getName),
      columnsToSelect.indexOf(col(ColumnName.TS_INSERIMENTO.getName)))

    tdErrorDescriptionColumns
      .foldLeft(rawDfPlusTrustedColumnsOpt.get
        .filter(getErrorFilterConditionCol(specifications)))((df, tuple2) => {

        df.withColumn(tuple2._1, tuple2._2)
      })
      .withColumn(ColumnName.ERROR_DESCRIPTION.getName,
        createErrorDescriptionCol(array(specifications
          .filter(x => x.colonnaRd.nonEmpty)
          .map(x => col(x.colonnaTd + "_error")): _*)))
      .select(columnsToSelectPlusErrorDescription: _*)
  }

  private def getErrorFilterConditionCol(specificationRecords: Seq[SpecificationRecord]): Column = {

    specificationRecords
      .filter(_.colonnaRd.nonEmpty)
      .map(x => x.areSomeRwColumnsNull || x.isTrdColumnNullWhileRwColumnsAreNot)
      .reduce(_ || _)
  }

  private def getDuplicatesRecordDf(specifications: Seq[SpecificationRecord]): DataFrame = {

    val primaryKeyColumns: Seq[Column] = specifications
      .filter(_.flagPrimaryKey.nonEmpty)
      .map(x => col(x.colonnaTd))

    val trdDfSelectCols: Seq[Column] = getColsToSelect(specifications,
      None,
      s => s.posizioneFinale,
      s => col(s.colonnaTd))

    val trustedCleanDf: DataFrame =  rawDfPlusTrustedColumnsOpt.get
      .filter(!getErrorFilterConditionCol(specifications))
      .select(trdDfSelectCols: _*)

    val duplicatesDfSelectCols: Seq[Column] = insertElementAtIndex(trdDfSelectCols,
      col(ColumnName.ROW_COUNT.getName),
      trdDfSelectCols.indexOf(col(ColumnName.TS_INSERIMENTO.getName)))

    trustedCleanDf
      .withColumn(ColumnName.ROW_COUNT.getName, count("*") over Window.partitionBy(primaryKeyColumns: _*))
      .filter(col(ColumnName.ROW_COUNT.getName) > 1)
      .select(duplicatesDfSelectCols: _*)
      .sort(primaryKeyColumns: _*)
  }
}

