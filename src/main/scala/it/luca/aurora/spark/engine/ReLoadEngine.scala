package it.luca.aurora.spark.engine

import it.luca.aurora.option.ScoptParser.ReloadConfig
import it.luca.aurora.option.{Branch, ScoptOption}
import it.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow}
import it.luca.aurora.utils.{ColumnName, TableId}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

class ReLoadEngine(applicationPropertiesFile: String)
  extends AbstractInitialOrReloadEngine(applicationPropertiesFile) {

  private final val logger = Logger.getLogger(getClass)
  private final val createReLoadLogRecord = createLogRecord(Branch.Reload.name, None, None, _: String, _: String, _: Option[String])

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

      val getOldActualDf: String => DataFrame = actualTable => {

        readFromJDBC(pcAuroraDBName, actualTable)
          .withColumn(ColumnName.TsFineValidita.name, lit(getJavaSQLTimestampFromNow))
          .withColumn(ColumnName.DtFineValidita.name, lit(getJavaSQLDateFromNow))
      }

      val readTSVAndUpdateVersionNumber: (String, String) => DataFrame = (tableId, oldVersionNumber) => {

        val newSpecificationVersion: String = f"${oldVersionNumber.toDouble + 0.1}%.1f"
        logger.info(f"Old specification number: '$oldVersionNumber'. Overriding with version number '$newSpecificationVersion'")

        readTsvAsDataframe(tableId)
          .withColumn(ColumnName.Versione.name, lit(newSpecificationVersion))
      }

      // Execute following operations according to selected flags
      // [a] insert data stored on actual table into historical table
      // [b] overwrite actual table

      val seqOfTablesToReload: Seq[(Boolean, (String, String, String))] = Seq(

        (mappingSpecificationFlag, (mappingSpecificationTBLName,
          jobProperties.getString("table.mapping_specification_historical.name"),
          TableId.MappingSpecification.tableId)),

        (lookupFlag, (lookupTBLName,
          jobProperties.getString("table.lookup_historical.name"),
          TableId.Lookup.tableId))
      )

      seqOfTablesToReload
        .filter(_._1)
        .foreach(t => {

          val actualTable: String = t._2._1
          val historicalTable: String = t._2._2
          val tableId: String = t._2._3

          logger.info(s"Starting to override table '$pcAuroraDBName'.'$actualTable' " +
            s"and save overwritten data into '$pcAuroraDBName'.'$historicalTable'")

          // [a] insert data stored on actual table into historical table
          writeToJDBCAndLog[String](pcAuroraDBName,
            historicalTable,
            SaveMode.Append,
            completeOverwriteFlag,
            createReLoadLogRecord,
            getOldActualDf,
            actualTable)

          // [b] overwrite actual table
          val oldVersionNumber: String = readFromJDBC(pcAuroraDBName, actualTable)
            .selectExpr(ColumnName.Versione.name)
            .distinct()
            .collect()(0)
            .getAs[String](0)

          writeToJDBCAndLog[String](pcAuroraDBName,
            actualTable,
            SaveMode.Overwrite,
            completeOverwriteFlag,
            createReLoadLogRecord,
            readTSVAndUpdateVersionNumber(_, oldVersionNumber),
            dataframeFunctionArg = tableId)
        })
    }
  }
}
