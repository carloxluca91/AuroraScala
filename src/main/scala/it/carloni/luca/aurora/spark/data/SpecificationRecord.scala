package it.carloni.luca.aurora.spark.data

case class SpecificationRecord(sorgente_rd: String,
                               tabella_td: String,
                               colonna_rd: String,
                               tipo_colonna_rd: String,
                               flag_discard: Option[String],
                               function_1: Option[String],
                               function_2: Option[String],
                               function_3: Option[String],
                               function_4: Option[String],
                               function_5: Option[String],
                               colonna_td: String,
                               tipo_colonna_td: String,
                               posizione_finale: Int,
                               primary_key: Option[String])
