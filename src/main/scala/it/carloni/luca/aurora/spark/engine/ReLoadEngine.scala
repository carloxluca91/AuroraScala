package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.option.{Branch, ScoptOption}
import it.carloni.luca.aurora.spark.data.LogRecord
import it.carloni.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

class ReLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createReLoadLogRecord = createLogRecord(Branch.ReLoad.toString, None, None, _: String, _: Option[String])

  def run(mappingSpecificationFlag: Boolean, lookupFlag: Boolean, completeOverwriteFlag: Boolean): Unit = {

    if (!Seq(mappingSpecificationFlag, lookupFlag)
      .reduce(_ | _)) {

      logger.warn("According to user input, no table has to be overriden. Thus, not much to do")
      logger.warn(s"To override mapping specification table, you must specify -${ScoptOption.mappingSpecificationFlag.short} " +
        s"(or -- ${ScoptOption.mappingSpecificationFlag.long})")
      logger.warn(s"To override look up table, you must specify -${ScoptOption.lookUpSpecificationFlag.short} " +
        s"(or -- ${ScoptOption.lookUpSpecificationFlag.long})")
    }

    else {

      val mappingSpecificationLoggingRecords: Seq[LogRecord] =
        if (mappingSpecificationFlag) {

          logger.info(s"Starting to overwrite table '$pcAuroraDBName'.'$mappingSpecificationTBLName'")
          reloadMappingSpecification(completeOverwriteFlag)

        } else Seq.empty

      val lookUpLoggingRecords: Seq[LogRecord] =
        if (lookupFlag) {

          logger.info(s"Starting to overwrite table '$pcAuroraDBName'.'$lookupTBLName'")
          mappingSpecificationLoggingRecords ++ reloadLookUpSpecification(completeOverwriteFlag)

        } else mappingSpecificationLoggingRecords

      writeLogRecords(lookUpLoggingRecords)
    }
  }

  private def reloadMappingSpecification(completeOverwrite: Boolean): Seq[LogRecord] = {

    val mappingSpecificationHistTBLName: String = jobProperties.getString("table.mapping_specification_historical.name")
    val oldMappingSpecificationDf: DataFrame = readFromJDBC(pcAuroraDBName, mappingSpecificationTBLName)
      .withColumn("ts_fine_validita", lit(getJavaSQLTimestampFromNow))
      .withColumn("dt_fine_validita", lit(getJavaSQLDateFromNow))

    val oldSpecificationVersion: Double = oldMappingSpecificationDf.selectExpr("versione")
      .distinct()
      .collect()(0)
      .getDouble(0)

    val newSpecificationVersion: Double = updateVersionNumber(oldSpecificationVersion)
    val mappingHistoricalLogRecord: LogRecord =
      Try(writeToJDBC(oldMappingSpecificationDf, pcAuroraDBName, mappingSpecificationHistTBLName, SaveMode.Append)) match {

        case Failure(exception) => createReLoadLogRecord(mappingSpecificationHistTBLName, Some(exception.getMessage))
        case Success(_) => createReLoadLogRecord(mappingSpecificationHistTBLName, None)
      }

    val mappingActualLogRecord: LogRecord =
      Try(writeToJDBC(this.getMappingSpecificationDfWithVersion(newSpecificationVersion), pcAuroraDBName, mappingSpecificationTBLName, SaveMode.Overwrite, completeOverwrite)) match {

        case Failure(exception) => createReLoadLogRecord(mappingSpecificationTBLName, Some(exception.getMessage))
        case Success(_) => createReLoadLogRecord(mappingSpecificationTBLName, None)
      }

    Seq(mappingHistoricalLogRecord, mappingActualLogRecord)
  }

  private def reloadLookUpSpecification(completeOverwrite: Boolean): Seq[LogRecord] = {

    val lookUpHistoricalTable: String = jobProperties.getString("table.lookup_historical.name")
    val oldLookUpDf: DataFrame = readFromJDBC(pcAuroraDBName, lookupTBLName)
      .withColumn("ts_fine_validita", lit(getJavaSQLTimestampFromNow))
      .withColumn("dt_fine_validita", lit(getJavaSQLDateFromNow))

    val oldSpecificationVersion: Double = oldLookUpDf.selectExpr("versione")
      .distinct()
      .collect()(0)
      .getDouble(0)

    val newSpecificationVersion: Double = updateVersionNumber(oldSpecificationVersion)
    val lookUpHistoricalLogRecord: LogRecord =
      Try(writeToJDBC(oldLookUpDf, pcAuroraDBName, lookUpHistoricalTable, SaveMode.Append)) match {

        case Failure(exception) => createReLoadLogRecord(lookUpHistoricalTable, Some(exception.getMessage))
        case Success(_) => createReLoadLogRecord(lookUpHistoricalTable, None)
      }

    val lookUpActualLogRecord: LogRecord =
      Try(writeToJDBC(this.getLookUpDfWithVersion(newSpecificationVersion), pcAuroraDBName, lookupTBLName, SaveMode.Overwrite, completeOverwrite)) match {

        case Failure(exception) => createReLoadLogRecord(lookupTBLName, Some(exception.getMessage))
        case Success(_) => createReLoadLogRecord(lookupTBLName, None)
      }

    Seq(lookUpHistoricalLogRecord, lookUpActualLogRecord)
  }

  private def getMappingSpecificationDfWithVersion(versionNumber: Double): DataFrame = {

    super.getMappingSpecificationDf
      .withColumn("versione", lit(versionNumber))
  }

  private def getLookUpDfWithVersion(versionNumber: Double): DataFrame = {

    super.getLookUpDf
      .withColumn("versione", lit(versionNumber))
  }

  private def updateVersionNumber(oldVersionNumber: Double): Double = {

    val newSpecificationVersion: Double = f"${oldVersionNumber + 0.1}%.1f"
      .replace(',', '.')
      .toDouble

    val oldVersionNumberStr: String = f"$oldVersionNumber%.1f"
      .replace(',', '.')

    logger.info(f"Old specification version: '$oldVersionNumberStr'. Overriding with version '$newSpecificationVersion'")
    newSpecificationVersion
  }
}
