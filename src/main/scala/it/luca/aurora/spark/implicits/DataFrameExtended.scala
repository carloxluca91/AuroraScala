package it.luca.aurora.spark.implicits

import it.luca.aurora.enumeration.ColumnName
import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.Utils.{now, toDate}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

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
    log.info(
      s"""Saving data to Hive table $fqTableName
         |
         |    ${df.schema.treeString}
         |""".stripMargin)
    val writer = df.write.mode(saveMode)
    if (df.sqlContext.tableExistsInDb(tableName, dbName)) {

      log.info(s"Hive table $fqTableName already exists. Saving data using .insertInto with saveMode $saveMode")
      writer.insertInto(fqTableName)
    } else {

      log.warn(s"Hive table $fqTableName does not exist yet. Creating now using .saveAsTable")
      writer.partitionBy(partitionBy)
        .saveAsTable(fqTableName)
    }

    log.info(s"Saved data to Hive table $fqTableName")
  }

  /**
   * Add a column next right to another
   * @param colName: new column name
   * @param column: new column expression
   * @param afterColName: name of column after which new column will be placed
   * @return dataframe with new column next right to afterColName
   */

  def withColumnAfter(colName: String, column: Column, afterColName: String): DataFrame = {
    withColumnAt(colName, column, df.columns.indexOf(afterColName) + 1)
  }

  /**
   * Add a column at position pos
   * @param colName: new column name
   * @param column: new column expression
   * @param pos: column position (0-indexed)
   * @return dataframe with new column at position pos
   */

  def withColumnAt(colName: String, column: Column, pos: Int): DataFrame = {

    val (leftSideColumns, rightSideColumns) = df.columns.splitAt(pos)
    val columns: Seq[Column] = leftSideColumns.map(col) ++ (column.as(colName) :: Nil) ++ rightSideColumns.map(col)
    df.select(columns: _*)
  }

  /**
   * Add a column next left to another
   * @param colName: new column name
   * @param column: new column expression
   * @param beforeColName: name of column before which new column will be placed
   * @return dataframe with new column next left to beforeColName
   */

  def withColumnBefore(colName: String, column: Column, beforeColName: String): DataFrame = {
    withColumnAt(colName, column, df.columns.indexOf(beforeColName))
  }

  private def renameColumns(regex: util.matching.Regex, matchF: util.matching.Regex.Match => String): DataFrame = {

    df.columns.foldLeft(df) { case (caseDf, columnName) =>
      val newColumnName: String = regex.replaceAllIn(columnName, matchF)
      caseDf.withColumnRenamed(columnName, newColumnName)
    }
  }

  def withJavaNamingConvention(): DataFrame = renameColumns("_([a-z])".r, m => s"${m.group(1).toUpperCase}")

  /**
   * Rename all dataframe columns using SQL naming convention
   * @return dataframe with renamed columns (e.g. "applicationStartTime" is renamed to "application_start_time")
   */

  def withSqlNamingConvention(): DataFrame = renameColumns("([A-Z])".r, m => s"_${m.group(1).toLowerCase}")

  /**
   * Add common technical columns
   * @return dataframe with technical columns
   */

  def withTechnicalColumns(): DataFrame = {

    df.withColumn(ColumnName.TsInsert, lit(now()))
      .withColumn(ColumnName.DtInsert, lit(toDate(now())))
      .withColumn(ColumnName.ApplicationId, lit(df.sqlContext.sparkContext.applicationId))
      .withColumn(ColumnName.ApplicationUser, lit(df.sqlContext.sparkContext.sparkUser))
      .withColumn(ColumnName.ApplicationType, lit("SPARK"))
  }
}
