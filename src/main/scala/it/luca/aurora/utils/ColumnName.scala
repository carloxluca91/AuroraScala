package it.luca.aurora.utils

object ColumnName extends Enumeration {

  protected case class Val(name: String)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToColumnNameVal(x: Value): Val = x.asInstanceOf[Val]

  // SPECIFICATION
  val Flusso: Val = Val("flusso")
  val Versione: Val = Val("versione")
  val DtFineValidita: Val = Val("dt_fine_validita")
  val DT_INIZIO_VALIDITA: Val = Val("dt_inizio_validita")
  val DT_INSERIMENTO: Val = Val("dt_inserimento")
  val DT_RIFERIMENTO: Val = Val("dt_riferimento")
  val ERROR_DESCRIPTION: Val = Val("descrizione_errore")
  val ROW_COUNT: Val = Val("row_count")
  val ROW_ID: Val = Val("row_id")
  val TS_INIZIO_VALIDITA: Val = Val("ts_inizio_validita")
  val TS_INSERIMENTO: Val = Val("ts_inserimento")
  val TS_FINE_VALIDITA: Val = Val("ts_fine_validita")

  // LOOKUP
  val LOOKUP_TIPO: Val = Val("tipo_lookup")
  val LOOKUP_ID: Val = Val("lookup_id")
  val LOOKUP_VALORE_ORIGINALE: Val = Val("valore_originale")
  val LOOKUP_VALORE_SOSTITUZIONE: Val = Val("valore_sostituzione")

}
