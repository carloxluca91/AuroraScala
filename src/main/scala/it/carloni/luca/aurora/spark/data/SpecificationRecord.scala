package it.carloni.luca.aurora.spark.data

import it.carloni.luca.aurora.spark.functions.Signature

case class SpecificationRecord(flusso: String,
                               sorgenteRd: String,
                               tabellaTd: String,
                               colonnaRd: String,
                               tipoColonnaRd: String,
                               posizioneIniziale: Int,
                               flagDiscardOpt: Option[String],
                               funzioneEtlOpt: Option[String],
                               flagLookupOpt: Option[String],
                               colonnaTd: String,
                               tipoColonnaTd: String,
                               posizioneFinale: Int,
                               flagPrimaryKeyOpt: Option[String]) {

  def involvesOtherColumns: (Boolean, Option[Seq[String]]) = {

    val falseReturnValue: (Boolean, Option[Seq[String]]) = (false, None)
    if (funzioneEtlOpt.nonEmpty) {

      val funzioneEtlValue: String = funzioneEtlOpt.get
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

  def involvesLookUp: Boolean = flagLookupOpt.nonEmpty

  def hasToBeDiscarded: Boolean = flagDiscardOpt.nonEmpty

  def involvesATransformation: Boolean = funzioneEtlOpt.nonEmpty

  def involvesRenaming: Boolean = !(colonnaRd equalsIgnoreCase colonnaTd)

  def involvesCasting: Boolean = !(tipoColonnaRd equalsIgnoreCase tipoColonnaTd)
}
