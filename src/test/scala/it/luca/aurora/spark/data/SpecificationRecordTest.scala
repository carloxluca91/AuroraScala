package it.luca.aurora.spark.data

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SpecificationRecordTest extends FunSuite with BeforeAndAfterEach {

  var specificationRecord: SpecificationRecord = _

  override def beforeEach() {

    specificationRecord = SpecificationRecord(flusso = "flusso",
      sorgenteRd = "sorgenteRd",
      tabellaTd = "tabellaTd",
      colonnaRd = Some("colonnaRd"),
      tipoColonnaRd = Some("tipoColonnaRd"),
      posizioneIniziale = Some(1),
      flagDiscard = None,
      funzioneEtl = Some("to_timestamp(lconcat_ws(@, col('data_movimento'), ' '), 'dd/MM/yyyy HH:mm:ss')"),
      flagLookup = None,
      tipoLookup = None,
      lookupId = None,
      colonnaTd = "colonnaTd",
      tipoColonnaTd = "tipoColonnatd",
      posizioneFinale = 1,
      flagPrimaryKey = None)
  }

  test("testInvolvesOtherColumns") {

    val (t1, t2): (Boolean, Option[Seq[String]]) = specificationRecord.involvesOtherRwColumns
    assertResult(true)(t1)
    assertResult(true)(t2.nonEmpty)
    assertResult(1)(t2.get.size)
    assertResult("data_movimento")(t2.get.head)
  }
}
