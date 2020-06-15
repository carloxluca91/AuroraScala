package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.spark.exception.{MultipleRdSourceException, MultipleTrdDestinationException, NoSpecificationException}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

class SourceLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(bancllName: String): Unit = {

    logger.info(s"Provided BANCLL name: $bancllName")

    // TRY TO GET TABLE CONTAINING INGESTION SPECIFICATION
    val mappingSpecification: DataFrame = readFromJDBC(pcAuroraDBName, mappingSpecificationTBLName)
    val rawSRCSpecificationRows: List[Row] = mappingSpecification
      .filter(col("flusso") === bancllName)
      .collect()
      .toList

    // CHECK IF CURRENT BANCLL IS DEFINED
    if (rawSRCSpecificationRows.nonEmpty) {

      logger.info(s"Identified ${rawSRCSpecificationRows.length} row(s) related to BANCLL $bancllName")
      val rawSRCTableNames: List[String] = rawSRCSpecificationRows
        .map(row => row.getAs[String]("sorgente_rd"))
        .distinct

      val trustedTableNames: List[String] = rawSRCSpecificationRows
        .map(row => row.getAs[String]("tabella_td"))
        .distinct

      // CHECK THAT ONLY 1 RAW TABLE AND ONLY 1 TRUSTED TABLE HAVE BEEN SPECIFIED FOR THIS BANCLL
      if (rawSRCTableNames.length.equals(1) && trustedTableNames.length.equals(1)) {

        val rawBancllTblName: String = rawSRCTableNames.head
        val trustedTableName: String = trustedTableNames.head

        logger.info(s"Raw table for BANCLL $bancllName: $rawBancllTblName")
        logger.info(s"Trusted table for BANCLL $rawBancllTblName: $trustedTableName")

        // TRY TO GET RAW TABLE
        val rawSourceDataFrame: DataFrame = readFromJDBC(lakeCedacriDBName, rawBancllTblName)

      }

      else {

        logger.error(s"Multiple sources or destination found within specification of BANCLL $bancllName")
        val exceptionToThrow: Exception = if (!rawSRCTableNames.length.equals(1)) {

          logger.error(s"Multiple sources found within specification of BANCLL $bancllName")
          new MultipleRdSourceException(bancllName, rawSRCTableNames)
        }

        else {

          logger.error(s"Multiple destination found within specification of BANCLL $bancllName")
          new MultipleTrdDestinationException(bancllName, trustedTableNames)
        }

        throw exceptionToThrow
      }
    }
    else {

      logger.error(s"Unable to retrieve any specification for BANCLL $bancllName. Related exception will be thrown")
      throw new NoSpecificationException(bancllName)
    }
  }
}
