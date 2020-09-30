package it.carloni.luca.aurora.spark.engine

import java.time.LocalDate

import it.carloni.luca.aurora.option.Branch
import it.carloni.luca.aurora.option.ScoptParser.SourceLoadConfig
import it.carloni.luca.aurora.spark.data.{LogRecord, SpecificationRecord}
import it.carloni.luca.aurora.spark.exception.{MultipleSrcOrDstException, NoSpecificationException}
import it.carloni.luca.aurora.spark.functions.ETLFunctionFactory
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
        val rwHistoricalTableName: String = rwActualTableName.concat("_h")
        val trdActualTableName: String = dstTables.head.toLowerCase
        val trdHistoricalTableName: String = trdActualTableName.concat("_h")

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
              .select(getColsToSelect(specificationRecords, s => col(s.colonnaTd)): _*)},

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
                .select(getColsToSelect(specificationRecords, s => col(s.colonnaTd)): _*)},
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
      "posizione_iniziale", "funzione_etl", "flag_lookup", "colonna_td", "tipo_colonna_td", "posizione_finale", "flag_primary_key")

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

      // RW_ERROR, TRD_ERROR, TRD_DUPLICATED

      (lakeCedacriDBName, rwActualTableName.concat("_error")) -> (getErrorDf(_, s => col(s.colonnaRd))),
      (pcAuroraDBName, trdActualTableName.concat("_error")) -> (getErrorDf(_, s => col(s.colonnaTd))),
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

  private def getColsToSelect(specificationRecords: Seq[SpecificationRecord], op: SpecificationRecord => Column): Seq[Column] = {

    // DEPENDING ON THE PROVIDED op, DEFINES SET OF RW OR TRUSTED COLUMNS TO SELECT

    (col(ColumnName.ROW_ID.getName)
      +: specificationRecords.map(op)) ++ Seq(col(ColumnName.TS_INSERIMENTO.getName),
      col(ColumnName.DT_INSERIMENTO.getName),
      col(ColumnName.DT_RIFERIMENTO.getName))
  }

  private def persistRawDfPlusTrustedColumns(rawDf: DataFrame, specificationRecords: Seq[SpecificationRecord]): Unit = {

    // ENRICH RAW DATAFRAME WITH COLUMNS DERIVED FROM SPECIFICATIONS
    val trustedColumns: Seq[(Column, String)] = deriveTrustedColumns(specificationRecords)
    val rawDfPlusTrustedColumns: DataFrame = trustedColumns
      .foldLeft(rawDf)((df, tuple2) => df.withColumn(tuple2._2, tuple2._1))

      // TECHNICAL COLUMNS
      .withColumn(ColumnName.TS_INSERIMENTO.getName, lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DT_INSERIMENTO.getName, lit(getJavaSQLDateFromNow))
      .persist()

    rawDfPlusTrustedColumnsOpt = Some(rawDfPlusTrustedColumns)
  }

  private def deriveTrustedColumns(specificationRecords: Seq[SpecificationRecord]): Seq[(Column, String)] = {

    val sourceName: String = specificationRecords.map(_.flusso).distinct.head
    lazy val lookUpDataFrame: DataFrame = readFromJDBC(pcAuroraDBName, lookupTBLName)
      .filter(col(ColumnName.FLUSSO.getName) === sourceName)
      .persist()

    specificationRecords
      .sortBy(_.posizioneFinale)
      .map((specificationRecord: SpecificationRecord) => {

        val rwColumnName: String = specificationRecord.colonnaRd
        val rwColumn: Column = col(rwColumnName)

        logger.info(s"Analyzing specification for raw column '$rwColumnName'")

        val trustedColumnBeforeLK: Column = if (!specificationRecord.involvesATransformation) {

          // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT NEEDS TO BE KEPT
          logger.info(s"No transformation to apply to raw column '$rwColumnName'")

          // CHECK IF INPUT DATATYPE MATCHES WITH OUTPUT DATATYPE
          val rwColumnType: String = specificationRecord.tipoColonnaRd
          val trdColumnType: String = specificationRecord.tipoColonnaTd
          if (!specificationRecord.involvesCasting) {

            logger.info(s"No type conversion to apply to raw column '$rwColumnName' " +
              s"(Raw data type: '$rwColumnType', trusted data type: '$trdColumnType')")
            rwColumn

          } else {

            // OTHERWISE, APPLY CASTING
            logger.info(s"Defining conversion for raw column '$rwColumnName' (from '$rwColumnType' to '$trdColumnType')")
            rwColumn.cast(resolveDataType(trdColumnType))
          }
        } else {

          // OTHERWISE, THE COLUMN IMPLIES SOME TRANSFORMATION
          ETLFunctionFactory(specificationRecord.funzioneEtl.get, rwColumn)
        }

        // CHECK LOOKUP FLAG
        val flagLookUp: Boolean = if (!specificationRecord.involvesLookUp) false else {

          val flagLookUpStr: String = specificationRecord.flagLookup.get
          logger.info(f"Lookup flag for column '${specificationRecord.colonnaRd}': '$flagLookUpStr''")
          flagLookUpStr equalsIgnoreCase "y"
        }

        // IF true, DEFINE A CASE-WHEN COLUMN ABLE TO CATCH PROVIDED CASES
        val trustedColumnAfterLK: Column = if (flagLookUp) {

          val lookUpCaseRows: Seq[Row] = lookUpDataFrame
            .filter(lower(col("nome_colonna")) === specificationRecord.colonnaTd.toLowerCase)
            .select("valore_originale", "valore_sostituzione")
            .collect()

          val firstLookUpCase: Row = lookUpCaseRows.head
          val foldLeftSeedCol: Column = when(trustedColumnBeforeLK === firstLookUpCase.get(0), firstLookUpCase.get(1))
          lookUpCaseRows.tail
            .foldLeft(foldLeftSeedCol)((col1, row) =>
              col1.when(trustedColumnBeforeLK === row.get(0), row.get(1)))

        } else trustedColumnBeforeLK

        (trustedColumnAfterLK, specificationRecord.colonnaTd)
      })
  }

  private def getErrorDf(specifications: Seq[SpecificationRecord], op: SpecificationRecord => Column): DataFrame = {

    val tdErrorColumns: Seq[(String, Column)] = specifications
      .map(x => {

        val (areOtherRwColumnsInvolved, rwInvolvedColumnNamesOpt): (Boolean, Option[Seq[String]]) = x.involvesOtherRwColumns
        val allOfRwColumnsInvolved: Seq[String] = if (areOtherRwColumnsInvolved) {

          x.colonnaRd +: rwInvolvedColumnNamesOpt.get
        } else x.colonnaRd :: Nil

        (x.colonnaTd + "_error", when(x.areSomeRwColumnsNull,
          createNullColumnsDescription(array(allOfRwColumnsInvolved.map(lit): _*), array(allOfRwColumnsInvolved.map(col): _*)))
          .when(x.isTrdColumnNullWhileRwColumnsAreNot,
            createNotNullColumnsDescription(array(allOfRwColumnsInvolved.map(lit): _*), array(allOfRwColumnsInvolved.map(col): _*))))
      })

    // GET INITIAL SET OF COLUMNS AND THEN ADD ERROR DESCRIPTION COLUMN (BEFORE 'ts_inserimento')
    val columnsToSelect: Seq[Column] = getColsToSelect(specifications, op)
    val columnsToSelectPlusErrorDescription: Seq[Column] = insertElementAtIndex(columnsToSelect,
      col(ColumnName.ERROR_DESCRIPTION.getName),
      columnsToSelect.indexOf(col(ColumnName.TS_INSERIMENTO.getName)))

    tdErrorColumns
      .foldLeft(rawDfPlusTrustedColumnsOpt.get
        .filter(getErrorFilterConditionCol(specifications)))((df, tuple2) => {

        df.withColumn(tuple2._1, tuple2._2)
      })
      .withColumn(ColumnName.ERROR_DESCRIPTION.getName,
        createErrorDescriptionCol(array(specifications.map(x => col(x.colonnaTd + "_error")): _*)))
      .select(columnsToSelectPlusErrorDescription: _*)
  }

  private def getErrorFilterConditionCol(specificationRecords: Seq[SpecificationRecord]): Column = {

    specificationRecords
      .map(x => x.areSomeRwColumnsNull || x.isTrdColumnNullWhileRwColumnsAreNot)
      .reduce(_ || _)
  }

  private def getDuplicatesRecordDf(specifications: Seq[SpecificationRecord]): DataFrame = {

    val primaryKeyColumns: Seq[Column] = specifications
      .filter(_.flagPrimaryKey.nonEmpty)
      .map(x => col(x.colonnaTd))

    val trdDfSelectCols: Seq[Column] = getColsToSelect(specifications, s => col(s.colonnaTd))
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

