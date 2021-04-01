package it.luca.aurora.spark.step

import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

case class WriteDf(private val inputDfKey: String,
                   private val dbName: String,
                   private val tableNameOrInputKey: String,
                   private val isTableName: Boolean,
                   private val saveMode: SaveMode,
                   private val partitionByOpt: Option[Seq[String]])
  extends IStep[DataFrame](s"WRITE_TABLE_${dbName.toUpperCase}.${tableNameOrInputKey.toUpperCase}") {

  override def run(variables: mutable.Map[String, Any]): Unit = {

    val dfToWrite = variables(inputDfKey).asInstanceOf[DataFrame]
    val tableName = if (isTableName) {
      tableNameOrInputKey
    } else {
      variables(tableNameOrInputKey).asInstanceOf[String]
    }

    dfToWrite.saveAsTableOrInsertInto(dbName, tableName, saveMode, partitionByOpt)
  }
}
