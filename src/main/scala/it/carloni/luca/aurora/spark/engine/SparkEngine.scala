package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.spark.exceptions.NoSpecificationException
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

class SparkEngine(applicationPropertiesFile: String)
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

        //TODO: eccezione MultipleRawSourceException
      }
    }
    else throw new NoSpecificationException(bancllName)
  }
}
