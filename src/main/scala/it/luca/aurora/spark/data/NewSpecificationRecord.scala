package it.luca.aurora.spark.data

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, lit, udf, when}

case class NewSpecificationRecord(flusso: String,
                                  sorgenteRd: String,
                                  tabellaTd: String,
                                  colonnaTd: String,
                                  posizioneFinale: Int,
                                  flagPrimaryKey: Option[String],
                                  colonneRd: Option[String],
                                  valoreDefault: Option[String],
                                  funzioneEtl: Option[String],
                                  flagLookup: Option[String],
                                  tipoLookup: Option[String],
                                  lookupId: Option[String],
                                  flagDescrizioneLookup: Option[String],
                                  nomeDescrizioneLookup: Option[String],
                                  posizioneDescrizioneLookup: Option[Int]) {

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

  def isPrimaryKey: Boolean = isFlagTrue(flagPrimaryKey)

  def doesIncludeLookupDescription: Boolean = isFlagTrue(flagDescrizioneLookup)

  def erroDescriptionColumn: Option[(String, Column)] = {

    inputRawColumns match {
      case None => None
      case Some(s) =>

        val rwColumns: Seq[Column] = s.map(col)
        val isAnyRwColumnNull: Column = rwColumns
          .map(_.isNull)
          .reduce(_ || _)

        val areAllRwColumnsNotNullButTrdColumnDoesNot: Column = rwColumns
          .map(_.isNotNull)
          .reduce(_ && _) && col(colonnaTd).isNull

        val errorDescriptionColumn: Column = when(isAnyRwColumnNull, writeNullableColumnNames(array(s.map(lit): _*), array(rwColumns: _*)))
          .when(areAllRwColumnsNotNullButTrdColumnDoesNot, writeAllRawColumnNamesAndValues(array(s.map(lit): _*), array(rwColumns: _*)))

        Some(s"${colonnaTd}_error", errorDescriptionColumn)
    }
  }

  def inputRawColumns: Option[Seq[String]] = {

    colonneRd match {
      case None => None
      case Some(x) => Some(x.split(", ").toSeq)
    }
  }

}
