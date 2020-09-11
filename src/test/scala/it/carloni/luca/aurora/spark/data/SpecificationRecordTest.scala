package it.carloni.luca.aurora.spark.data

import org.scalatest.{BeforeAndAfterEach, FunSuite}

class SpecificationRecordTest extends FunSuite with BeforeAndAfterEach {

  var specificationRecord: SpecificationRecord = _

  override def beforeEach() {

    specificationRecord = SpecificationRecord(flusso = "flusso",
      sorgenteRd = "sorgenteRd",
      tabellaTd = "tabellaTd",
      colonnaRd = "colonnaRd",
      tipoColonnaRd = "tipoColonnaRd",
      posizioneIniziale = 1,
      flagDiscardOpt = None,
      funzioneEtlOpt = Some("to_timestamp(lconcat_ws(@, col('data_movimento'), ' '), 'dd/MM/yyyy HH:mm:ss')"),
      flagLookupOpt = None,
      colonnaTd = "colonnaTd",
      tipoColonnaTd = "tipoColonnatd",
      posizioneFinale = 1,
      flagPrimaryKeyOpt = None)
  }

  test("testInvolvesOtherColumns") {

    val (t1, t2): (Boolean, Option[Seq[String]]) = specificationRecord.involvesOtherColumns
    assertResult(true)(t1)
    assertResult(true)(t2.nonEmpty)
    assertResult(1)(t2.get.size)
    assertResult("data_movimento")(t2.get.head)
  }

  test("involvesRenaming") {

    assertResult(true)(specificationRecord.involvesRenaming)
  }

  test("involvesCasting") {

    assertResult(true)(specificationRecord.involvesCasting)
  }
}
