package it.carloni.luca.aurora.spark.engine

import java.time.LocalDate

import it.carloni.luca.aurora.option.ScoptParser.SourceLoadConfig
import it.carloni.luca.aurora.spark.data.SpecificationRecord
import it.carloni.luca.aurora.spark.exception.{MultipleRdSourceException, MultipleTrdDestinationException, NoSpecificationException}
import it.carloni.luca.aurora.spark.functions.ETLFunctionFactory
import it.carloni.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow, resolveDataType}
import it.carloni.luca.aurora.utils.{ColumnName, DateFormat}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SaveMode}

class SourceLoadEngine(private final val applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(sourceLoadConfig: SourceLoadConfig): Unit = {

    import sparkSession.implicits._

    val bancllName: String = sourceLoadConfig.bancllName
    val versionNumberOpt: Option[Double] = sourceLoadConfig.versionNumberOpt
    val dtBusinessDateOpt: Option[String] = sourceLoadConfig.dtRiferimentoOpt

    logger.info(s"Provided BANCLL name: '$bancllName'")
    val mappingSpecificationFilterColumn: Column = if (versionNumberOpt.isEmpty) {

      logger.info("No specification version number has been provided. Using latest version number")
      col("flusso") === bancllName

    } else {

      val versionNumber: Double = versionNumberOpt.get
      val versionNumberFormatted: String = f"$versionNumber%.1f"
        .replace(',', '.')

      logger.info(f"Specification version number to be used: '$versionNumberFormatted")
      (col("flusso") === bancllName) &&
        (col("versione") === versionNumber)
    }

    // TRY TO GET TABLE CONTAINING INGESTION SPECIFICATION
    val mappingSpecification: DataFrame = readFromJDBC(pcAuroraDBName, mappingSpecificationTBLName)
    val specificationRecords: List[SpecificationRecord] = mappingSpecification
      .filter(mappingSpecificationFilterColumn)
      .selectExpr("flusso", "sorgente_rd", "tabella_td", "colonna_rd", "tipo_colonna_rd", "flag_discard", "posizione_iniziale",
        "funzione_etl", "flag_lookup", "colonna_td", "tipo_colonna_td", "posizione_finale", "flag_primary_key")
      .as[SpecificationRecord]
      .collect()
      .toList

    logger.info(f"Successfully parsed dataframe as a set of elements of type ${SpecificationRecord.getClass.getSimpleName}")

    // CHECK IF CURRENT BANCLL IS DEFINED
    if (specificationRecords.nonEmpty) {

      logger.info(s"Identified ${specificationRecords.length} row(s) related to BANCLL '$bancllName'")
      val rawSRCTableNames: List[String] = specificationRecords
        .map(_.sorgente_rd)
        .distinct

      val trustedTableNames: List[String] = specificationRecords
        .map(_.tabella_td)
        .distinct

      // CHECK THAT ONLY 1 RAW TABLE AND ONLY 1 TRUSTED TABLE HAVE BEEN SPECIFIED FOR THIS BANCLL
      if (rawSRCTableNames.length.equals(1) && trustedTableNames.length.equals(1)) {

        val rawActualTableName: String = rawSRCTableNames.head.toLowerCase
        val rawHistoricalTableName: String = rawActualTableName.concat("_h")
        val trustedActualTableName: String = trustedTableNames.head.toLowerCase
        val trustedHistoricalTableName: String = trustedActualTableName.concat("_h")

        logger.info(s"BANCLL '$bancllName' -> Raw actual table: '$rawActualTableName', Raw historical table: '$rawHistoricalTableName'")
        logger.info(s"BANCLL '$bancllName' -> Trusted actual table: '$trustedActualTableName', Trusted historical table: '$trustedHistoricalTableName'")

        // RETRIEVE DATA TO BE PROCESSED
        val rawSourceDataFrame: DataFrame = if (dtBusinessDateOpt.nonEmpty) {

          // IF A dt_riferimento HAS BEEN PROVIDED, READ FROM RAW_HISTORICAL_TABLE
          val dtRiferimentoStr: String = dtBusinessDateOpt.get
          logger.info(s"Provided business date: '$dtRiferimentoStr'. Thus, reading raw data from '$rawHistoricalTableName'")

          val dtRiferimentoSQLDate: java.sql.Date =  java.sql.Date.valueOf(LocalDate.parse(dtRiferimentoStr, DateFormat.DT_RIFERIMENTO.getFormatter))
          readFromJDBC(lakeCedacriDBName, rawHistoricalTableName)
            .filter(col(ColumnName.DT_RIFERIMENTO.getName) === dtRiferimentoSQLDate)

        } else {

          // OTHERWISE, READ FROM RAW_ACTUAL_TABLE
          logger.info(s"No business date has been provided. Thus, reading raw data from '$rawActualTableName'")
          readFromJDBC(lakeCedacriDBName, rawActualTableName)
        }

        writeOutput(rawSourceDataFrame, specificationRecords)

      } else {

        // DETECT THE EXCEPTION TO BE THROWN: MULTIPLE SOURCES OR MULTIPLE DESTINATIONS ?
        logger.error(s"Multiple sources or destination found within specification of BANCLL $bancllName")
        if (!rawSRCTableNames.length.equals(1)) {

          throw new MultipleRdSourceException(bancllName, rawSRCTableNames)
        } else {

          throw new MultipleTrdDestinationException(bancllName, trustedTableNames)
        }
      }
    } else {

      // NO SPECIFICATION FOUND
      throw new NoSpecificationException(bancllName)
    }
  }

  private def writeOutput(rawDataFrame: DataFrame, specificationRecords: List[SpecificationRecord]): Unit = {

    val trustedColumns: Seq[(Column, String)] = deriveTrustedColumns(specificationRecords)

    // ENRICH RAW DATAFRAME WITH COLUMNS DERIVED FROM SPECIFICATIONS
    val rawDfPlusTrustedColumns: DataFrame = trustedColumns
      .foldLeft(rawDataFrame)((df, tuple2) => df.withColumn(tuple2._2, tuple2._1))

      // TECHNICAL COLUMNS
      .withColumn(ColumnName.TS_INSERIMENTO.getName, lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DT_INSERIMENTO.getName, lit(getJavaSQLDateFromNow))

    val errorDfFilterCol: Column = specificationRecords
      .map(specificationRecord => {col(specificationRecord.colonna_rd).isNotNull && col(specificationRecord.colonna_td).isNull})
      .reduce(_ || _)

    // RAW LAYER, ERROR DATAFRAME
    val rawErrorDf: DataFrame = rawDfPlusTrustedColumns
      .filter(errorDfFilterCol)
      .selectExpr(rawDataFrame.columns: _*)
      .sort(ColumnName.ROW_ID.getName)

    val rawErrorActualTableName: String = specificationRecords.map(_.sorgente_rd.toLowerCase)
      .distinct
      .head
      .concat("_error")

    writeBothActualAndHistoricalData(rawErrorDf, lakeCedacriDBName, rawErrorActualTableName, "RAW", "ERROR")

    val trustedDfSelectCols: Seq[Column] = (col(ColumnName.ROW_ID.getName)
      +: trustedColumns.map(x => col(x._2))) ++ Seq(col(ColumnName.TS_INSERIMENTO.getName),
      col(ColumnName.DT_INSERIMENTO.getName),
      col(ColumnName.DT_RIFERIMENTO.getName))

    // TRUSTED LAYER, CLEAN DATAFRAME
    val trustedCleanDf: DataFrame = rawDfPlusTrustedColumns
      .filter(!errorDfFilterCol)
      .select(trustedDfSelectCols: _*)
      .sort(ColumnName.ROW_ID.getName)

    val trustedCleanTableName: String = specificationRecords.map(_.tabella_td.toLowerCase)
      .distinct
      .head

    writeBothActualAndHistoricalData(trustedCleanDf, pcAuroraDBName, trustedCleanTableName, "TRUSTED", "CLEAN")

    // TRUSTED LAYER, ERROR DATAFRAME
    val trustedErrorDf: DataFrame = rawDfPlusTrustedColumns
      .filter(errorDfFilterCol)
      .select(trustedDfSelectCols: _*)
      .sort(ColumnName.ROW_ID.getName)

    val trustedErrorTableName: String = trustedCleanTableName.concat("_error")
    writeBothActualAndHistoricalData(trustedErrorDf, pcAuroraDBName, trustedErrorTableName, "TRUSTED", "ERROR")

    val primaryKeyColumns: Seq[Column] = specificationRecords
      .filter(_.flag_primary_key.nonEmpty)
      .map(x => col(x.colonna_td))

    val duplicatesDfSelectCols: Seq[Column] = (trustedDfSelectCols.take(trustedDfSelectCols.indexOf(col(ColumnName.DT_RIFERIMENTO.getName)))
      :+ col(ColumnName.ROW_COUNT.getName)) :+ col(ColumnName.DT_RIFERIMENTO.getName)

    // TRUSTED LAYER, DPLICATES FROM CLEAN DATAFRAME
    val duplicatesDf: DataFrame = trustedCleanDf
      .withColumn(ColumnName.ROW_COUNT.getName, count("*") over Window.partitionBy(primaryKeyColumns: _*))
      .filter(col(ColumnName.ROW_COUNT.getName) > 1)
      .select(duplicatesDfSelectCols: _*)
      .sort(primaryKeyColumns: _*)

    val trustedCleanDuplicatesTableName: String = trustedCleanTableName.concat("_duplicated")
    writeBothActualAndHistoricalData(duplicatesDf, pcAuroraDBName, trustedCleanDuplicatesTableName, "TRUSTED", "DUPLICATED")
  }

  private def deriveTrustedColumns(specificationRecords: Seq[SpecificationRecord]): Seq[(Column, String)] = {

    val sourceName: String = specificationRecords.map(_.flusso).distinct.head
    lazy val lookUpDataFrame: DataFrame = readFromJDBC(pcAuroraDBName, lookupTBLName)
      .filter(col("flusso") === sourceName)
      .persist()

    specificationRecords
      .sortBy(_.posizione_finale)
      .map((specificationRecord: SpecificationRecord) => {

        val rawColumnName: String = specificationRecord.colonna_rd
        val rawColumn: Column = col(rawColumnName)

        logger.info(s"Analyzing specification for raw column '$rawColumnName'")

        val trustedColumnBeforeLK: Column = if (specificationRecord.funzione_etl.isEmpty && specificationRecord.flag_discard.isEmpty) {

          // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT NEEDS TO BE KEPT
          logger.info(s"No transformation to apply to raw column '$rawColumnName'")

          // CHECK IF INPUT DATATYPE MATCHES OUTPUT DATATYPE
          val rawColumnType: String = specificationRecord.tipo_colonna_rd
          val trustedColumnType: String = specificationRecord.tipo_colonna_td
          if (rawColumnType.equalsIgnoreCase(trustedColumnType)) {

            // IF THEY DO, NO CASTING IS NEEDED
            logger.info(s"No type conversion to apply to raw column '$rawColumnName' " +
              s"(Raw data type: '$rawColumnType', trusted data type: '$trustedColumnType')")
            rawColumn

          } else {

            // OTHERWISE, APPLY CASTING
            logger.info(s"Defining conversion for raw column '$rawColumnName' (from '$rawColumnType' to '$trustedColumnType')")
            rawColumn.cast(resolveDataType(trustedColumnType))
          }
        } else {

          // OTHERWISE, THE COLUMN IMPLIES SOME TRANSFORMATION
          ETLFunctionFactory(specificationRecord.funzione_etl.get, rawColumn)
        }

        // CHECK LOOKUP FLAG
        val flagLookUp: Boolean = if (specificationRecord.flag_lookup.isEmpty) false else {

          val flagLookUpStr: String = specificationRecord.flag_lookup.get
          logger.info(f"Lookup flag: $flagLookUpStr")
          if (flagLookUpStr.equalsIgnoreCase("y")) true else false
        }

        // IF true, DEFINE A CASE-WHEN COLUMN ABLE TO CATCH PROVIDED CASES
        val trustedColumnAfterLK: Column = if (flagLookUp) {

          val lookUpCaseRows: Seq[Row] = lookUpDataFrame
            .filter(lower(col("nome_colonna")) === specificationRecord.colonna_td.toLowerCase)
            .select("valore_originale", "valore_sostituzione")
            .collect()

          lookUpCaseRows.tail
            .foldLeft(when(trustedColumnBeforeLK === lookUpCaseRows.head.get(0), lookUpCaseRows.head.get(1)))(
              (col1, row) => col1.when(trustedColumnBeforeLK === row.get(0), row.get(1)))
            .otherwise(null)

        } else trustedColumnBeforeLK

        (trustedColumnAfterLK, specificationRecord.colonna_td)
      })
  }

  private def writeBothActualAndHistoricalData(dataFrame: DataFrame, database: String, actualTableName: String,
                                               layer: String, description: String): Unit = {

    val historicalTableName: String = actualTableName.concat("_h")
    logger.info(s"Saving data related to layer '$layer' ($description). Actual table name: '$actualTableName', historical: '$historicalTableName'")
    writeToJDBC(dataFrame, database, actualTableName, SaveMode.Overwrite)
    writeToJDBC(dataFrame, database, historicalTableName, SaveMode.Append)

  }
}
