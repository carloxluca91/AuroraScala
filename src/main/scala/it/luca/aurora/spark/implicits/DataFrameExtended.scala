package it.luca.aurora.spark.implicits

import it.luca.aurora.logging.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Date, Timestamp}

class DataFrameExtended(private val df: DataFrame)
  extends Logging {

  /**
   * Save dataframe using .saveAsTable if given dbName.tableName does not exists, using .insertInto otherwise
   * @param dbName: Hive db name
   * @param tableName: Hive table name
   * @param saveMode: dataframe saveMode
   * @param partitionBy: partitioning columns (if any)
   */

  def saveAsTableOrInsertInto(dbName: String, tableName: String, saveMode: SaveMode, partitionBy: Option[Seq[String]]): Unit = {

    val fqTableName = s"$dbName.$tableName"
    log.info(s"Saving data to Hive table $fqTableName")
    if (df.sqlContext.tableExistsInDb(tableName, dbName)) {

      log.info(s"Hive table $fqTableName already exists. Saving data using .insertInto with saveMode $saveMode")
      df.write
        .mode(saveMode)
        .insertInto(fqTableName)
    } else {

      log.warn(s"Hive table $fqTableName does not exist yet. Creating now using .saveAsTable")
      df.write
        .mode(saveMode)
        .partitionBy(partitionBy)
        .saveAsTable(fqTableName)
    }

    log.info(s"Saved data to Hive table $fqTableName")
  }

  /**
   * Rename all dataframe columns using SQL naming convention (e.g. "applicationStartTime" is renamed to "application_start_time")
   * @return dataframe with renamed columns
   */

  def withSqlNamingConvention(): DataFrame = {

    val regex: util.matching.Regex = "([A-Z])".r
    df.columns.foldLeft(df) { case (caseDf, columnName) =>

      val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
      caseDf.withColumnRenamed(columnName, newColumnName)
    }
  }

  /**
   * Add common technical columns
   * @return dataframe with technical columns
   */

  def withTechnicalColumns(): DataFrame = {

    df.withColumn("ts_insert", lit(new Timestamp(System.currentTimeMillis())))
      .withColumn("dt_insert", lit(new Date(System.currentTimeMillis())))
      .withColumn("application_id", lit(df.sqlContext.sparkContext.applicationId))
      .withColumn("application_user", lit(df.sqlContext.sparkContext.sparkUser))
      .withColumn("application_type", lit("SPARK"))
  }
}
