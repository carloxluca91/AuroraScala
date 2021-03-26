package it.luca.aurora.spark.step

import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.{DataFrame, SaveMode}

case class WriteDf(override protected val input: DataFrame,
                   private val dbName: String,
                   private val tableName: String,
                   private val saveMode: SaveMode,
                   private val partitionByOpt: Option[Seq[String]])
  extends IStep[DataFrame](input, stepName =  s"WRITE_DATAFRAME $dbName.$tableName") {

  override def run(): Unit = input.saveAsTableOrInsertInto(dbName, tableName, saveMode, partitionByOpt)
}
