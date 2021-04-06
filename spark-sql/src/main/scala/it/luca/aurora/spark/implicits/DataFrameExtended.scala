package it.luca.aurora.spark.implicits

import it.luca.aurora.Logging
import it.luca.aurora.utils.{now, toDate}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, SaveMode}

import java.sql.Connection

class DataFrameExtended(private val df: DataFrame)
  extends Logging {

  def getCreateTableStatement(dbName: String, tableName: String, partitionBy: Seq[String]): String = {

    val isAPartitioningCol: StructField => Boolean = s => partitionBy.contains(s.name)
    val columnSqlDefinition: StructField => String = s => s"${s.name} ${s.dataType.simpleString}"
    val tableDefinitionColumns: String = df.schema.fields.filterNot { isAPartitioningCol }
      .map { columnSqlDefinition }
      .mkString(",\n  ")
    val partitioningClause: String = df.schema.fields.filter { isAPartitioningCol }
      .map { columnSqlDefinition }
      .mkString(", ")

    s"""CREATE TABLE IF NOT EXISTS $dbName.$tableName (
       |
       |  $tableDefinitionColumns
       |)
       |PARTITIONED BY ($partitioningClause)
       |STORED AS PARQUET
       |""".stripMargin
  }

  /**
   * Save dataframe using .saveAsTable if given dbName.tableName does not exists, using .insertInto otherwise
   * @param dbName: Hive db name
   * @param tableName: Hive table name
   * @param saveMode: dataframe saveMode
   * @param partitionByOpt: partitioning columns (if any)
   */

  def saveAsOrInsertInto(dbName: String, tableName: String,
                         saveMode: SaveMode,
                         partitionByOpt: Option[Seq[String]],
                         connection: Connection): Unit = {

    val (sqlContext, fqTableName) = (df.sqlContext, s"$dbName.$tableName")
    val dfWriter: DataFrameWriter = df.write.mode(saveMode).partitionBy(partitionByOpt)
    log.info(
      s"""Saving data to Hive table $fqTableName
         |
         |${df.schema.treeString}
         |""".stripMargin)

    val impalaStatement: String = if (sqlContext.existsTable(tableName, dbName)) {
      log.info(s"Hive table $fqTableName already exists. Saving data using .insertInto with saveMode $saveMode")
      dfWriter.insertInto(fqTableName)
      saveMode match {
        case SaveMode.Overwrite => s"INVALIDATE METADATA $fqTableName"
        case _ => s"REFRESH $fqTableName"
      }
    } else {

      log.warn(s"Hive table $fqTableName does not exist yet")
      if (partitionByOpt.isDefined) {

        // We need to pre-create Hive table using HiveQL
        val createTable: String = getCreateTableStatement(dbName, tableName, partitionByOpt.get)
        log.info(
          s"""Creating Hive table $fqTableName using statement
             |
             |$createTable
             |""".stripMargin)

        sqlContext.sql(createTable)
        log.info(s"Created Hive table $fqTableName")
        dfWriter.insertInto(fqTableName)
      } else dfWriter.saveAsTable(fqTableName)

      s"INVALIDATE METADATA $fqTableName"
    }

    // Issue Impala statement
    log.info(s"Saved data to Hive table $fqTableName")
    connection.createStatement().executeUpdate(impalaStatement)
    log.info(s"Executed statement '$impalaStatement'")
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

    df.withColumn("ts_insert", lit(now()))
      .withColumn("dt_insert", lit(toDate(now(), "yyyy-MM-dd")))
      .withColumn("application_id", lit(df.sqlContext.sparkContext.applicationId))
      .withColumn("application_user", lit(df.sqlContext.sparkContext.sparkUser))
      .withColumn("application_type", lit("SPARK"))
  }
}
