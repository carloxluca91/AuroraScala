package it.luca.aurora.spark.engine

import it.luca.aurora.enumeration.{Branch, ScoptOption}
import it.luca.aurora.option.ReloadConfig
import it.luca.aurora.utils.ColumnName
import it.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/*
case class ReLoadEngine(override protected val sqlContext: SQLContext,
                        override val propertiesFile: String)
  extends AbstractEngine(sqlContext, propertiesFile, Branch.Reload) {

  private final val logger = Logger.getLogger(getClass)
  private final val createReLoadLogRecord = LogRecord(sparkSession.sparkContext, Branch.Reload.name, None, None,
    _: String, _: String, _: Option[String])

  private final val getActualDfPlusFineValidita: String => DataFrame = actualTable => {

    readFromJDBC(pcAuroraDBName, actualTable)
      .withColumn(ColumnName.TsFineValidita.name, lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DtFineValidita.name, lit(getJavaSQLDateFromNow))
  }

  // Function for reading new .tsv file as Dataframe with updated version number
  private final val readTSVAndUpdateVersionNumber: String => DataFrame = actualTable => {

    val oldVersionNumber: String = readFromJDBC(pcAuroraDBName, actualTable)
      .selectExpr(ColumnName.Versione.name)
      .distinct()
      .collect()(0)
      .getAs[String](0)
      .replace(",", ".")

    val newSpecificationVersion: String = f"${oldVersionNumber.toDouble + 0.1}%.1f"
      .replace(",", ".")
    logger.info(f"Old specification number: '$oldVersionNumber'. Overriding with version number '$newSpecificationVersion'")

    readTsvAsDataframe(actualTable)
      .withColumn(ColumnName.Versione.name, lit(newSpecificationVersion))
  }

  def run(reloadConfig: ReloadConfig): Unit = {

    val mappingSpecificationFlag: Boolean = reloadConfig.mappingSpecificationFlag
    val lookupFlag: Boolean = reloadConfig.lookUpFlag
    val completeOverwriteFlag: Boolean = reloadConfig.completeOverwriteFlag

    // If none of the two flags has been selected
    if (!(mappingSpecificationFlag || lookupFlag)) {

      logger.warn("According to user input, no table has to be overriden. Thus, not much to do")
      logger.warn(s"To override mapping specification table, you must specify -${ScoptOption.MappingSpecificationFlag.shortOption} " +
        s"(or -- ${ScoptOption.MappingSpecificationFlag.longOption})")
      logger.warn(s"To override look up table, you must specify -${ScoptOption.LookupSpecificationFlag.shortOption} " +
        s"(or -- ${ScoptOption.LookupSpecificationFlag.longOption})")

    } else {

      // Execute following operations according to selected flags
      // [a] insert data stored on actual table into historical table
      // [b] overwrite actual table

      val reloadTables: Map[String, Boolean] = Map(
        mappingSpecificationTBLName -> mappingSpecificationFlag,
        lookupTBLName -> lookupFlag)

      reloadTables
        .filter(t => t._2)
        .foreach(t => {

          val (actualTable, _): (String, Boolean) = t
          val historicalTable = s"${actualTable}_h"
          logger.info(s"Starting to insert old data on table '$pcAuroraDBName.$historicalTable' and overwrite table '$pcAuroraDBName.$actualTable'")

          // [a] insert data stored on actual table into historical table
          writeToJDBCAndLog[String](pcAuroraDBName,
            historicalTable,
            SaveMode.Append,
            truncateFlag = false,
            createReLoadLogRecord,
            getActualDfPlusFineValidita,
            actualTable)

          writeToJDBCAndLog[String](pcAuroraDBName,
            actualTable,
            SaveMode.Overwrite,
            completeOverwriteFlag,
            createReLoadLogRecord,
            readTSVAndUpdateVersionNumber,
            actualTable)
        })
    }
  }
}

 */
