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
import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{Column, DataFrame}

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
      .selectExpr("sorgente_rd", "tabella_td", "colonna_rd", "tipo_colonna_rd", "flag_discard", "posizione_iniziale",
        "funzione_etl", "flag_lookup", "colonna_td", "tipo_colonna_td", "posizione_finale", "flag_primary_key")
      .as[SpecificationRecord]
      .collect()
      .toList

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

          // IF A dt_business_date HAS BEEN PROVIDED, READ FROM RAW_HISTORICAL_TABLE
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

    val trustedColumns: Seq[(Column, String)] = specificationRecords
      .sortBy(_.posizione_finale)
      .map((specificationRecord: SpecificationRecord) => {

        val rawColumnName: String = specificationRecord.colonna_rd
        val rawColumn: Column = col(rawColumnName)

        logger.info(s"Analyzing specification for raw column '$rawColumnName'")

        val trustedColumn: Column = if (specificationRecord.funzione_etl.isEmpty && specificationRecord.flag_discard.isEmpty) {

          // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT NEEDS TO BE KEPT
          logger.info(s"No transformation to apply to raw column '$rawColumnName'")

          // CHECK IF INPUT DATATYPE MATCHES OUTPUT DATATYPE
          val rawColumnType: String = specificationRecord.tipo_colonna_rd
          val trustedColumnType: String = specificationRecord.tipo_colonna_td
          if (rawColumnType.equalsIgnoreCase(trustedColumnType)) {

            // IF THEY DO, NO CASTING IS NEEDED
            logger.info(s"No type conversion to apply to raw column '$rawColumnName'. Raw type: '$rawColumnType', trusted type: '$trustedColumnType'")
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

        (trustedColumn, specificationRecord.colonna_td)
      })

    val rowIdColumnName: String = ColumnName.ROW_ID.getName
    val dtRiferimentoColumnName: String = ColumnName.DT_RIFERIMENTO.getName
    val rowCountColumnName: String = ColumnName.ROW_COUNT.getName
    val tsInserimentoColumnName: String = ColumnName.TS_INSERIMENTO.getName
    val dtInserimentoColumnName: String = ColumnName.DT_INSERIMENTO.getName

    val rawDfPlusTrustedColumns: DataFrame = trustedColumns
      .foldLeft(rawDataFrame)((df, tuple2) => df.withColumn(tuple2._2, tuple2._1))
      .withColumn(tsInserimentoColumnName, lit(getJavaSQLTimestampFromNow))
      .withColumn(dtInserimentoColumnName, lit(getJavaSQLDateFromNow))

    val errorDfFilterCol: Column = specificationRecords
      .map(specificationRecord => {

        val rwCol: Column = col(specificationRecord.colonna_rd)
        val trdCol: Column = col(specificationRecord.colonna_td)
        rwCol.isNotNull && trdCol.isNull
      })
      .reduce(_ || _)

    val rawErrorDf: DataFrame = rawDfPlusTrustedColumns
      .filter(errorDfFilterCol)
      .select(rawDataFrame.columns.head, rawDataFrame.columns.tail: _*)
      .sort(rowIdColumnName)

    val trustedDfSelectCols: Seq[Column] = (col(rowIdColumnName)
      +: trustedColumns.map(x => col(x._2))) ++ Seq(col(tsInserimentoColumnName), col(dtInserimentoColumnName), col(dtRiferimentoColumnName))

    val trustedCleanDf: DataFrame = rawDfPlusTrustedColumns
      .filter(!errorDfFilterCol)
      .select(trustedDfSelectCols: _*)
      .sort(rowIdColumnName)

    val trustedErrorDf: DataFrame = rawDfPlusTrustedColumns
      .filter(errorDfFilterCol)
      .select(trustedDfSelectCols: _*)
      .sort(rowIdColumnName)

    val primaryKeyColumns: Seq[Column] = specificationRecords
      .filter(_.flag_primary_key.nonEmpty)
      .map(x => col(x.colonna_td))

    val duplicatesDfSelectCols: Seq[Column] = (trustedDfSelectCols.take(trustedDfSelectCols.indexOf(col(dtRiferimentoColumnName)))
      :+ col(rowCountColumnName)) :+ col(dtRiferimentoColumnName)

    val duplicatesDf: DataFrame = trustedCleanDf
      .withColumn(rowCountColumnName, count("*") over Window.partitionBy(primaryKeyColumns: _*))
      .filter(col(rowCountColumnName) > 1)
      .select(duplicatesDfSelectCols: _*)
      .sort(primaryKeyColumns: _*)
  }
}
