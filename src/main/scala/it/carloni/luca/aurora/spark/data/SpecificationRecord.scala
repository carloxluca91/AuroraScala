package it.carloni.luca.aurora.spark.data

case class SpecificationRecord(flusso: String,
                               sorgenteRd: String,
                               tabellaTd: String,
                               colonnaRd: String,
                               tipoColonnaRd: String,
                               posizioneIniziale: Int,
                               flagDiscard: Option[String],
                               funzioneEtl: Option[String],
                               flagLookup: Option[String],
                               colonnaTd: String,
                               tipoColonnaTd: String,
                               posizioneFinale: Int,
                               flagPrimaryKey: Option[String])
