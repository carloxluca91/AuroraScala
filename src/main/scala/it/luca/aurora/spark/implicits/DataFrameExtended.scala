package it.luca.aurora.spark.implicits

import grizzled.slf4j.Logging
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Date, Timestamp}

class DataFrameExtended(private val df: DataFrame)
  extends Logging {

  def saveAsTableOrInsertInto(dbName: String, tableName: String, saveMode: SaveMode, partitionBy: Option[Seq[String]]): Unit = {

    val fqTableName = s"$dbName.$tableName"
    info(s"Saving data to Hive table $fqTableName")
    if (df.sqlContext.tableExistsInDb(tableName, dbName)) {

      info(s"Hive table $fqTableName already exists. Saving data using .insertInto with saveMode $saveMode")
      df.write
        .mode(saveMode)
        .insertInto(fqTableName)
    } else {

      warn(s"Hive table $fqTableName does not exist yet. Creating now using .saveAsTable")
      df.write
        .mode(saveMode)
        .partitionBy(partitionBy)
        .saveAsTable(fqTableName)
    }

    info(s"Saved data to Hive table $fqTableName")
  }

  def withSqlNamingConvention(): DataFrame = {

    val regex: util.matching.Regex = "([A-Z])".r
    df.columns.foldLeft(df) { case (caseDf, columnName) =>

      val newColumnName: String = regex.replaceAllIn(columnName, m => s"_${m.group(1).toLowerCase}")
      caseDf.withColumnRenamed(columnName, newColumnName)
    }
  }

  def withTechnicalColumns(): DataFrame = {

    df.withColumn("ts_insert", lit(new Timestamp(System.currentTimeMillis())))
      .withColumn("dt_insert", lit(new Date(System.currentTimeMillis())))
      .withColumn("application_id", lit(df.sqlContext.sparkContext.applicationId))
      .withColumn("application_user", lit(df.sqlContext.sparkContext.sparkUser))
      .withColumn("application_type", lit("SPARK"))
  }
}
