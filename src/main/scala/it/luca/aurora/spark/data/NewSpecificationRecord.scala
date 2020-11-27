package it.luca.aurora.spark.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class NewSpecificationRecord(flusso: String,
                                  sorgenteRd: String,
                                  tabellaTd: String,
                                  colonnaTd: String,
                                  posizioneFinale: Int,
                                  flagPrimaryKey: Option[String],
                                  colonnaRd: Option[String],
                                  funzioneEtl: Option[String],
                                  flagLookup: Option[String],
                                  tipoLookup: Option[String],
                                  lookupId: Option[String]) {

  private final val writeNullableColumnNames: UserDefinedFunction =
    udf((columnNames: Seq[String], columnValues: Seq[Option[Any]]) => {

      columnNames.zip(columnValues)
        .filter(t => t._2.isEmpty)
        .map(t => s"${t._1} (null)")
        .mkString(", ")
    })

  private final val writeAllRawColumnNamesAndValues: UserDefinedFunction =
    udf((columnNames: Seq[String], columnValues: Seq[Option[Any]]) => {

      columnNames.zip(columnValues)
        .map(t => s"${t._1} (${t._2})")
        .mkString(", ")
    })

  private def isFlagTrue(opt: Option[String]): Boolean = {

    opt match {
      case None => false
      case Some(x) => x equalsIgnoreCase "y"
    }
  }

  def isPrimaryKeyFlagOn: Boolean = isFlagTrue(flagPrimaryKey)

  def isLookupFlagOn: Boolean = isFlagTrue(flagLookup)

  def errorCondition: Option[Column] = {

    inputRdColumns match {
      case None => None
      case Some(s) =>

        val isAnyRdColumnNull: Column = s
          .map(col(_).isNull)
          .reduce(_ || _)

        val doesInputMismatch: Column = s
          .map(col(_).isNotNull)
          .reduce(_ && _) && col(colonnaTd).isNull

        Some(isAnyRdColumnNull || doesInputMismatch)
    }
  }

  def errorDescription: Option[(String, Column)] = {

    inputRdColumns match {
      case None => None
      case Some(s) =>

        val rwColumns: Seq[Column] = s.map(col)
        val rwColumnNames: Seq[Column] = s.map(lit)
        val isAnyRdColumnNull: Column = s
        .map(col(_).isNull)
        .reduce(_ || _)

        val doesInputMismatch: Column = s
          .map(col(_).isNotNull)
          .reduce(_ && _) && col(colonnaTd).isNull

        val errorDescriptionColumn: Column = when(isAnyRdColumnNull, writeNullableColumnNames(array(rwColumnNames: _*), array(rwColumns: _*)))
          .when(doesInputMismatch, writeAllRawColumnNamesAndValues(array(rwColumnNames: _*), array(rwColumns: _*)))

        Some(s"${colonnaTd}_error", errorDescriptionColumn)
    }
  }

  def inputRdColumns: Option[Seq[String]] = {

    colonnaRd match {
      case None => None
      case Some(x) => Some(x.split(", ").toSeq)
    }
  }
}

object NewSpecificationRecord {

  val columnsToSelect: Seq[String] = Seq("flusso",
    "sorgente_rd",
    "tabella_td",
    "colonna_td",
    "posizione_finale",
    "flag_primary_key",
    "colonne_rd",
    "funzione_etl",
    "flag_lookup",
    "tipo_lookup",
    "lookup_id" )
}
