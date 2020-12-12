package it.luca.aurora.utils

object ColumnName extends Enumeration {

  protected case class Val(name: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToColumnNameVal(x: Value): Val = x.asInstanceOf[Val]

  // Mapping Specification and raw layer data
  val Flusso: Val = Val("flusso")
  val Versione: Val = Val("versione")
  val DtFineValidita: Val = Val("dt_fine_validita")
  val DtInizioValidita: Val = Val("dt_inizio_validita")
  val DtInserimento: Val = Val("dt_inserimento")
  val DtRiferimento: Val = Val("dt_riferimento")
  val ErrorDescription: Val = Val("descrizione_errore")
  val RowCount: Val = Val("row_count")
  val RowId: Val = Val("row_id")
  val TsInizioValidita: Val = Val("ts_inizio_validita")
  val TsInserimento: Val = Val("ts_inserimento")
  val TsFineValidita: Val = Val("ts_fine_validita")

  // Lookup
  val LookupTipo: Val = Val("tipo_lookup")
  val LookupId: Val = Val("lookup_id")
  val LookupValoreOriginale: Val = Val("valore_originale")
  val LookupValoreSostituzione: Val = Val("valore_sostituzione")

}
