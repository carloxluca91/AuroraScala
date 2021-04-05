package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.Connection
import scala.collection.mutable

case class WriteDf(private val inputDfKey: String,
                   private val dbName: String,
                   private val tableNameOrInputKey: String,
                   private val isTableName: Boolean,
                   private val saveMode: SaveMode,
                   private val partitionByOpt: Option[Seq[String]],
                   private val connection: Connection)
  extends IStep[DataFrame]("[Save|Insert] Dataframe [as|into] Hive table")
    with Logging {

  override def run(variables: mutable.Map[String, Any]): Unit = {

    val dfToWrite = variables(inputDfKey).asInstanceOf[DataFrame]
    val tableName = if (isTableName) tableNameOrInputKey else variables(tableNameOrInputKey).asInstanceOf[String]
    dfToWrite.saveAsOrInsertInto(dbName, tableName, saveMode, partitionByOpt, connection)
  }
}
