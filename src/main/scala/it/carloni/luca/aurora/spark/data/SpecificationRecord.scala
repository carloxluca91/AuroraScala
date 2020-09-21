package it.carloni.luca.aurora.spark.data

import it.carloni.luca.aurora.spark.functions.Signature

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
                               flagPrimaryKey: Option[String]) {

  def involvesOtherColumns: (Boolean, Option[Seq[String]]) = {

    val falseReturnValue: (Boolean, Option[Seq[String]]) = (false, None)
    if (funzioneEtl.nonEmpty) {

      val funzioneEtlValue: String = funzioneEtl.get
      val involvedColumnNames: Seq[String] = Signature.dfColOrLit
        .regex
        .findAllMatchIn(funzioneEtlValue)
        .filter(_.group(1) equalsIgnoreCase "col")
        .map(_.group(2))
        .toSeq

      if (involvedColumnNames.nonEmpty) {

        (true, Some(involvedColumnNames))

      } else falseReturnValue

    } else falseReturnValue
  }

  def involvesLookUp: Boolean = flagLookup.nonEmpty

  def hasToBeDiscarded: Boolean = flagDiscard.nonEmpty

  def involvesATransformation: Boolean = funzioneEtl.nonEmpty

  def involvesRenaming: Boolean = !(colonnaRd equalsIgnoreCase colonnaTd)

  def involvesCasting: Boolean = !(tipoColonnaRd equalsIgnoreCase tipoColonnaTd)
}
