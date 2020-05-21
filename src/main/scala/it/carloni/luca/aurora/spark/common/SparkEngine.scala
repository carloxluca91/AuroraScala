package it.carloni.luca.aurora.spark.common

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

class SparkEngine(val sparkContext: SparkContext, val applicationPropertiesFile: String)
  extends AbstractEngine(sparkContext, applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)

  def run(rawSRCName: String): Unit = {

    logger.info(s"Provided BANCLL name: $rawSRCName")

    // CHECK EXISTENCE OF TABLE CONTAINING INGESTION SPECIFICATION
    if (existsTableInDatabase(mappingSpecificationTBLName, pcAuroraDBName)) {

      val rawSRCSpecificationRows: List[Row] = hiveContext.table(mappingSpecificationFullTBLName)
        .filter(col("flusso") === rawSRCName)
        .collect()
        .toList

      // CHECK IF CURRENT BANCLL IS DEFINED
      if (rawSRCSpecificationRows.nonEmpty) {

        logger.info(s"Identified ${rawSRCSpecificationRows.length} row(s) related to BANCLL $rawSRCName")
        val rawSRCTableNames: List[String] = rawSRCSpecificationRows
          .map(row => row.getAs[String]("sorgente_rd"))
          .distinct

        val trustedTableNames: List[String] = rawSRCSpecificationRows
          .map(row => row.getAs[String]("tabella_td"))
          .distinct

        // CHECK THAT ONLY 1 RAW TABLE AND ONLY 1 TRUSTED TABLE HAVE BEEN SPECIFIED FOR THIS BANCLL
        if (rawSRCTableNames.length.equals(1) && trustedTableNames.length.equals(1)) {

          val rawSRCTableName: String = rawSRCTableNames.head
          val trustedTableName: String = trustedTableNames.head

          logger.info(s"Raw table for BANCLL $rawSRCName: $rawSRCTableName")
          logger.info(s"Trusted table for BANCLL $rawSRCTableName: $trustedTableName")

          // CHECK EXISTENCE OF RAW TABLE
          if (existsTableInDatabase(rawSRCTableName, lakeCedacriDBName)) {

            val rawSRCFullTableName: String = s"$lakeCedacriDBName.$rawSRCTableName"
            logger.info(s"Starting to process table $rawSRCFullTableName")
            val rawSourceDataFrame: DataFrame = hiveContext.table(rawSRCFullTableName)
          }

          else {

            logger.error(s"Raw source table for BANCCL $rawSRCName ($lakeCedacriDBName.$rawSRCTableName) does not exist yet")
            // TODO: lancio eccezione personalizzata
          }
        }

        else {

          logger.error(s"Multiple raw or trusted table have been specified for BANCLL $rawSRCName ")
          logger.error(s"Raw source tables: ${rawSRCTableNames.mkString(", ")}")
          logger.error(s"Trusted tables: ${trustedTableNames.mkString(", ")}")

          // TODO: lancio eccezione personalizzata
        }
      }

      else {

        logger.error(s"Provided BANCLL name ($rawSRCName) does not have any specification stated within table $mappingSpecificationFullTBLName")
        // TODO: lancio eccezione personalizzata
      }
    }

    else {

      logger.error(s"Table $pcAuroraDBName.$mappingSpecificationTBLName does not exist. Thus, nothing will be triggered")
      // TODO: lancio eccezione personalizzata
    }
  }
}
