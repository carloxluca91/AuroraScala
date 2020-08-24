package it.carloni.luca.aurora.spark.data

case class SpecificationRecord(flusso: String,
                               sorgente_rd: String,
                               tabella_td: String,
                               colonna_rd: String,
                               tipo_colonna_rd: String,
                               posizione_iniziale: Int,
                               flag_discard: Option[String],
                               funzione_etl: Option[String],
                               flag_lookup: Option[String],
                               colonna_td: String,
                               tipo_colonna_td: String,
                               posizione_finale: Int,
                               flag_primary_key: Option[String])
