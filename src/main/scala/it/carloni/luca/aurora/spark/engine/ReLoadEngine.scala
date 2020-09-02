package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.option.ScoptParser.ReloadConfig
import it.carloni.luca.aurora.option.{Branch, ScoptOption}
import it.carloni.luca.aurora.utils.ColumnName
import it.carloni.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class ReLoadEngine(applicationPropertiesFile: String)
  extends AbstractEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createReLoadLogRecord = createLogRecord(Branch.RE_LOAD.getName, None, None, _: String, _: Option[String])

  def run(reloadConfig: ReloadConfig): Unit = {

    val mappingSpecificationFlag: Boolean = reloadConfig.mappingSpecificationFlag
    val lookupFlag: Boolean = reloadConfig.lookUpFlag
    val completeOverwriteFlag: Boolean = reloadConfig.completeOverwriteFlag

    // IF NONE OF THE TWO FLAGS HAVE BEEN SELECTED
    if (!(mappingSpecificationFlag || lookupFlag)) {

      logger.warn("According to user input, no table has to be overriden. Thus, not much to do")
      logger.warn(s"To override mapping specification table, you must specify -${ScoptOption.MAPPING_SPECIFICATION_FLAG.getShortOption} " +
        s"(or -- ${ScoptOption.MAPPING_SPECIFICATION_FLAG.getLongOption})")
      logger.warn(s"To override look up table, you must specify -${ScoptOption.LOOKUP_SPECIFICATION_FLAG.getShortOption} " +
        s"(or -- ${ScoptOption.LOOKUP_SPECIFICATION_FLAG.getLongOption})")

    } else {

      // EXECUTE SELECTED FLAGS

      // Map(Boolean -> (spark.sql.DataFrame, String, String))
      Map(mappingSpecificationFlag -> (getMappingSpecificationDf,
        mappingSpecificationTBLName,
        jobProperties.getString("table.mapping_specification_historical.name")),

        lookupFlag -> (getLookUpDf,
          lookupTBLName,
          jobProperties.getString("table.lookup_historical.name")))

        .filter(_._1)
        .values
        .foreach(x => {

          val df: DataFrame = x._1
          val actualTableName: String = x._2
          val historicalTableName: String = x._3

          logger.info(s"Starting to overwrite table '$pcAuroraDBName'.'$actualTableName'")
          reloadTable(df,
            pcAuroraDBName,
            actualTableName,
            historicalTableName,
            completeOverwriteFlag)})
    }
  }

  private def reloadTable(newDf: DataFrame,
                          databaseName: String,
                          actualTableName: String,
                          historicalTableName: String,
                          completeOverwrite: Boolean): Unit = {

    val oldDf: DataFrame = readFromJDBC(databaseName, actualTableName)
      .withColumn("ts_fine_validita", lit(getJavaSQLTimestampFromNow))
      .withColumn("dt_fine_validita", lit(getJavaSQLDateFromNow))

    val oldDfVersionNumber: Double = oldDf
      .selectExpr(ColumnName.VERSIONE.getName)
      .distinct()
      .collect()(0)
      .getAs[Double](0)

    val newDfVersionNumber: Double = updateVersionNumber(oldDfVersionNumber)

    // INSERT OLD DATA INTO HISTORICAL TABLE (SAVEMODE = APPEND)
    tryWriteToJDBCAndLog(oldDf,
      databaseName,
      historicalTableName,
      SaveMode.Append,
      completeOverwrite,
      createReLoadLogRecord)

    // OVERWRITE ACTUAL TABLE (SAVEMODE = OVERWRITE)
    tryWriteToJDBCAndLog(updateDfVersionNumber(newDf, newDfVersionNumber),
      databaseName,
      actualTableName,
      SaveMode.Overwrite,
      completeOverwrite,
      createReLoadLogRecord)
  }

  private def updateDfVersionNumber(df: DataFrame, versionNumber: Double): DataFrame = {

    df.withColumn(ColumnName.VERSIONE.getName, lit(versionNumber))
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
