package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.option.ScoptParser.ReloadConfig
import it.carloni.luca.aurora.option.{Branch, ScoptOption}
import it.carloni.luca.aurora.utils.{ColumnName, TableId}
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

      // Function1[String, DataFrame]
      val getOldActualDf: String => DataFrame = actualTable => {

        readFromJDBC(pcAuroraDBName, actualTable)
          .withColumn("ts_fine_validita", lit(getJavaSQLTimestampFromNow))
          .withColumn("dt_fine_validita", lit(getJavaSQLDateFromNow))
      }

      // Function2[String, Double, DataFrame]
      val readTSVAndUpdateVersionNumber: (String, Double) => DataFrame = (tableId, versionNumber) => {

        readTSVForTable(tableId)
          .withColumn(ColumnName.VERSIONE.getName, lit(versionNumber))
      }

      // EXECUTE SELECTED FLAGS
      Map(mappingSpecificationFlag -> (mappingSpecificationTBLName,
        jobProperties.getString("table.mapping_specification_historical.name"),
        TableId.MAPPING_SPECIFICATION.getId),

      lookupFlag -> (lookupTBLName,
        jobProperties.getString("table.lookup_historical.name"),
        TableId.LOOK_UP.getId))

        .filter(_._1)
        .values
        .foreach(x => {

          val actualTable: String = x._1
          val historicalTable: String = x._2
          val tableId: String = x._3

          // WRITE OLD DATA ON HISTORICAL TABLE
          tryWriteToJDBCWithFunction1[String](pcAuroraDBName,
            historicalTable,
            SaveMode.Append,
            completeOverwriteFlag,
            createReLoadLogRecord,
            getOldActualDf,
            actualTable)

          // OVERWRITE ACTUAL TABLE
          // RETRIEVE OLD ACTUAL VERSION NUMBER
          val oldVersionNumber: Double = readFromJDBC(pcAuroraDBName, actualTable)
            .selectExpr(ColumnName.VERSIONE.getName)
            .distinct()
            .collect()(0)
            .getAs[Double](0)

          val nextDfVersionNumber: Double = getNextVersionNumber(oldVersionNumber)
          tryWriteToJDBCWithFunction1[String](pcAuroraDBName,
            actualTable,
            SaveMode.Overwrite,
            completeOverwriteFlag,
            createReLoadLogRecord,
            readTSVAndUpdateVersionNumber(_, nextDfVersionNumber),
            dfGenerationFunctionArg = tableId)
        })
    }
  }

  private def getNextVersionNumber(oldVersionNumber: Double): Double = {

    val newSpecificationVersion: Double = f"${oldVersionNumber + 0.1}%.1f"
      .replace(',', '.')
      .toDouble

    val oldVersionNumberStr: String = f"$oldVersionNumber%.1f"
      .replace(',', '.')

    logger.info(f"Old specification version: '$oldVersionNumberStr'. Overriding with version '$newSpecificationVersion'")
    newSpecificationVersion
  }
}
