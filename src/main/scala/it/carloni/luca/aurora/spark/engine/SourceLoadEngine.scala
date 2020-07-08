package it.carloni.luca.aurora.spark.engine

import java.sql.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.aurora.spark.data.SpecificationRecord
import it.carloni.luca.aurora.spark.exception.{MultipleRdSourceException, MultipleTrdDestinationException, NoSpecificationException}
import it.carloni.luca.aurora.utils.DateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

class SourceLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(bancllName: String, dtBusinessDateOpt: Option[String], versionNumberOpt: Option[Double]): Unit = {

    import sparkSession.implicits._

    logger.info(s"Provided BANCLL name: '$bancllName'")
    val mappingSpecificationFilterColumn: Column = versionNumberOpt match {

      case None =>

        logger.info("No specification version number has been provided. Using latest version number")
        col("flusso") === bancllName

      case Some(value) =>

        val versionNumberFormatted: String = f"$value%.1f"
          .replace(',', '.')

        logger.info(f"Specification version number to be used: '$versionNumberFormatted")
        (col("flusso") === bancllName) &&
          (col("version") === value)
    }

    // TRY TO GET TABLE CONTAINING INGESTION SPECIFICATION
    val mappingSpecification: DataFrame = readFromJDBC(pcAuroraDBName, mappingSpecificationTBLName)
    val specificationRecords: List[SpecificationRecord] = mappingSpecification
      .filter(mappingSpecificationFilterColumn)
      .selectExpr("sorgente_rd", "tabella_td", "colonna_rd", "tipo_colonna_rd", "flag_discard", "function_1",
        "function_2", "function_3", "function_4", "function_5", "colonna_td", "tipo_colonna_td", "posizione_finale",
        "primary_key")
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

        val rawActualTableName: String = rawSRCTableNames.head
        val rawHistoricalTableName: String = rawActualTableName.concat("_h")
        val trustedActualTableName: String = trustedTableNames.head
        val trustedHistoricalTableName: String = trustedActualTableName.concat("_h")

        logger.info(s"Raw actual table for BANCLL '$bancllName': '$rawActualTableName'")
        logger.info(s"Raw historical table for BANCLL '$bancllName': '$rawHistoricalTableName'")
        logger.info(s"Trusted actual table for BANCLL '$bancllName': '$trustedActualTableName'")
        logger.info(s"Trusted historical table for BANCLL '$bancllName': '$trustedHistoricalTableName'")

        // RETRIEVE DATA TO BE PROCESSED
        val rawSourceDataFrame: DataFrame = if (dtBusinessDateOpt.nonEmpty) {

          // IF A dt_business_date HAS BEEN PROVIDED, READ FROM RAW_HISTORICAL_TABLE
          val dtBusinessDateAsStr: String = dtBusinessDateOpt.get
          logger.info(s"Provided business date: '$dtBusinessDateAsStr'. Thus, reading raw data from '$rawHistoricalTableName'")
          val dtBusinessDateAsDate: java.sql.Date = fromStringToJavaSQLDate(dtBusinessDateAsStr, DateFormat.DtBusinessDate.format)
          readFromJDBC(lakeCedacriDBName, rawHistoricalTableName)
            .filter(col("dt_business_date") === dtBusinessDateAsDate)

        } else {

          // OTHERWISE, READ FROM RAW_ACTUAL_TABLE
          logger.info(s"No business date has been provided. Thus, reading raw data from '$rawActualTableName'")
          readFromJDBC(lakeCedacriDBName, rawActualTableName)
        }

      } else {

        logger.error(s"Multiple sources or destination found within specification of BANCLL $bancllName")
        val exceptionToThrow: Exception = if (!rawSRCTableNames.length.equals(1)) {

          logger.error(s"Multiple sources found within specification of BANCLL $bancllName")
          new MultipleRdSourceException(bancllName, rawSRCTableNames)

        } else {

          logger.error(s"Multiple destination found within specification of BANCLL $bancllName")
          new MultipleTrdDestinationException(bancllName, trustedTableNames)
        }

        throw exceptionToThrow
      }

    } else {

      logger.error(s"Unable to retrieve any specification for bancll '$bancllName'. Related exception will be thrown")
      throw new NoSpecificationException(bancllName)
    }
  }

  private def transformRawData(rawDataframe: DataFrame, specificationRecords: List[SpecificationRecord]): DataFrame = {

    val trustedColumns: Seq[Column] = specificationRecords
      .map((specificationRecord: SpecificationRecord) => {

        val rawColumnName: String = specificationRecord.colonna_rd

        logger.info(s"Analyzing specification for raw column '$rawColumnName'")
        val functionsToApply: Seq[String] = Seq(specificationRecord.function_1, specificationRecord.function_2,
          specificationRecord.function_3, specificationRecord.function_4, specificationRecord.function_5)
          .filter(_.nonEmpty)
          .map(_.get)

        // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT MUST TO BE KEPT
        if (functionsToApply.isEmpty && specificationRecord.flag_discard.isEmpty) {

          logger.info(s"No transformation to apply to raw column '$rawColumnName'")

          // CHECK IF INPUT AND OUTPUT DATATYPE MATCH
          val rawColumnType: String = specificationRecord.tipo_colonna_rd
          val trustedColumnType: String = specificationRecord.tipo_colonna_td
          val rawColumnCol: Column = if (rawColumnType.equalsIgnoreCase(trustedColumnType)) {

            logger.info(s"No type conversion to apply to raw column '$rawColumnName'. Raw type: '$rawColumnType', trusted type: '$trustedColumnType'")
            col(rawColumnName)

          } else {

            logger.info(s"Defining conversion for raw column '$rawColumnName' (from '$rawColumnType' to '$trustedColumnType')")
            col(rawColumnName).cast(resolveDataType(trustedColumnType))
          }

          rawColumnCol.alias(specificationRecord.colonna_td)

        } else {

          //TODO: applicazione funzioni
          col(rawColumnName)
        }
      })

    rawDataframe
  }

  private def fromStringToJavaSQLDate(date: String, dateFormat: String): java.sql.Date = {

    Date.valueOf(LocalDate.parse(date,
      DateTimeFormatter.ofPattern(dateFormat)))
  }
}
