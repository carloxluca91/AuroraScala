package it.carloni.luca.aurora.spark.data

import it.carloni.luca.aurora.spark.functions.etl.ETLSignatures
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class SpecificationRecord(flusso: String,
                               sorgenteRd: String,
                               tabellaTd: String,
                               colonnaRd: Option[String],
                               tipoColonnaRd: String,
                               posizioneIniziale: Int,
                               flagDiscard: Option[String],
                               funzioneEtl: Option[String],
                               flagLookup: Option[String],
                               colonnaTd: String,
                               tipoColonnaTd: String,
                               posizioneFinale: Int,
                               flagPrimaryKey: Option[String]) {

  def involvesOtherRwColumns: (Boolean, Option[Seq[String]]) = {

    val falseReturnValue: (Boolean, Option[Seq[String]]) = (false, None)
    if (involvesATransformation) {

      val funzioneEtlValue: String = funzioneEtl.get
      val involvedColumnNames: Seq[String] = ETLSignatures.dfColOrLit
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

  def involvesCasting: Boolean = !(tipoColonnaRd equalsIgnoreCase tipoColonnaTd)

  def areSomeRwColumnsNull: Column = {

    val (areOtherRwColumnsInvolved, otherInvolvedColumnsOpt): (Boolean, Option[Seq[String]]) = involvesOtherRwColumns
    val allRwColumnsInvolved: Seq[Column] = if (areOtherRwColumnsInvolved) {

      col(colonnaRd.get) +: otherInvolvedColumnsOpt
        .get
        .map(col)

    } else col(colonnaRd.get) :: Nil

    allRwColumnsInvolved
      .map(_.isNull)
      .reduce(_ || _)
  }

  def isTrdColumnNullWhileRwColumnsAreNot: Column = {

    val (areOtherRwColumnsInvolved, otherInvolvedColumnsOpt): (Boolean, Option[Seq[String]]) = involvesOtherRwColumns
    val allRwColumnsInvolved: Seq[Column] = if (areOtherRwColumnsInvolved) {

      col(colonnaRd.get) +: otherInvolvedColumnsOpt.get.map(col)

    } else col(colonnaRd.get) :: Nil

    allRwColumnsInvolved
      .map(_.isNotNull)
      .reduce(_ && _) && col(colonnaTd).isNull
  }
}
