package it.luca.aurora.spark.data

import it.luca.aurora.AbstractSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class SpecificationRecordSpec extends AbstractSpec {

  private final val (flusso, sorgenteRd, tabellaTd, colonnaTd, posizioneFinale) = ("flusso", "sorgenteRd", "tabellaTd", "trdCol1", 1)

  private def specificationRecordPartialApply(flagPrimaryKey: Option[String],
                                              colonnaRd: Option[String],
                                              funzioneEtl: Option[String],
                                              flagLookup: Option[String],
                                              tipoLookup: Option[String],
                                              lookupId: Option[String]): SpecificationRecord = {

      SpecificationRecord(flusso, sorgenteRd, tabellaTd, colonnaTd, posizioneFinale,
        flagPrimaryKey, colonnaRd, funzioneEtl, flagLookup, tipoLookup, lookupId)
    }

  s"A ${className[SpecificationRecord]} object" should
    "detect if both primaryKeyFlag and LookupFlag have been correctly set or not" in {

    val testSeq: Seq[((Option[String], Option[String]), (Boolean, Boolean))] =
      ((Some("Y"), Some("y")), (true, true)) ::
        ((Some("c"), Some("other")), (false, false)) ::
        Nil

    testSeq foreach { t =>

      val (flagPrimaryKeyOpt, flagLookupOpt) = t._1
      val (expectedResultFlagPrimaryKey, expectedResultFlagLookup) = t._2
      val specificationRecord = specificationRecordPartialApply(flagPrimaryKeyOpt, None, None, flagLookupOpt, None, None)
      assert(specificationRecord.isPrimaryKeyFlagOn == expectedResultFlagPrimaryKey)
      assert(specificationRecord.isLookupFlagOn == expectedResultFlagLookup)
    }
  }

  it should "detect specified raw input columns and compute a proper error condition" in {

    val (rwdCol1, rwdCol2) = ("rwdCol1", "rwdCol2")
    val testSeq: Seq[(Option[String], Int, Option[String], Column)] =
      (Some(rwdCol1), 1, None, col(rwdCol1).isNull) ::
        (Some(s"$rwdCol1, $rwdCol2"), 2, None, col(rwdCol1).isNull || col(rwdCol2).isNull) ::
        (Some(rwdCol1), 1, Some("a"), col(rwdCol1).isNull || (col(rwdCol1).isNotNull && col(colonnaTd).isNull)) :: Nil

    testSeq foreach { t =>

      val (rwColumnsOpt, expectedSize, funzioneEtlOpt, expectedErrorConditionColumn) = t
      val recordWithSingleRwColumn = specificationRecordPartialApply(None, rwColumnsOpt, funzioneEtlOpt, None, None, None)
      val inputRdColumnsOpt: Option[Seq[String]] = recordWithSingleRwColumn.inputRdColumns
      val errorConditionOpt: Option[Column] = recordWithSingleRwColumn.errorCondition
      assert(inputRdColumnsOpt.nonEmpty)
      assert(inputRdColumnsOpt.get.size == expectedSize)
      assert(errorConditionOpt.nonEmpty)
      assert(errorConditionOpt.get == expectedErrorConditionColumn)
    }
  }
}
