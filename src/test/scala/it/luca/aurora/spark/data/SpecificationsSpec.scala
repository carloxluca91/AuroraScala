package it.luca.aurora.spark.data

import it.luca.aurora.AbstractSpec
import it.luca.aurora.exception.{MultipleDstException, MultipleSrcException}
import it.luca.aurora.utils.ColumnName
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Try}

class SpecificationsSpec extends AbstractSpec {

  private final val (sorgenteRd1, sorgenteRd2) = ("sorgenteRd1", "sorgenteRd2")
  private final val (tabellaTd1, tabellaTd2) = ("tabellaTd1", "tabellaTd2")
  private final val (trdCol1, trdCol2, trdCol3) = ("trdCol1", "trdCol2", "trdCol3")
  private final val (rwdCol1, rwdCol2) = ("rwdCol1", "rwdCol2")

  private final val specificationRecordApply = SpecificationRecord("flusso", _: String, _: String, _: String, _: Int,
    _: Option[String], _: Option[String], None, None, None, None)

  private case class PartialSpecificationRecord(sorgenteRd: String,
                                                tabellaTd: String,
                                                colonnaTd: String,
                                                posizioneFinale: Int,
                                                primaryKeyOpt: Option[String],
                                                colonnaRdOpt: Option[String])

  private final val recordsSettings: Seq[(String, String, String, Int, Option[String], Option[String])] = Seq(
    (sorgenteRd1, tabellaTd1, trdCol1, 1, Some("Y"), Some(rwdCol1)),
    (sorgenteRd2, tabellaTd2, trdCol2, 2, None, Some(rwdCol2)),
    (sorgenteRd1, tabellaTd1, trdCol3, 3, Some("Y"), None))

  private final val partialSpecificationRecords = recordsSettings
    .map(t => {
      val (sorgenteRd, tabellaTd, colonnaTd, posizioneFinale, primaryKeyOpt, colonnaRdOpt) = t
      PartialSpecificationRecord(sorgenteRd, tabellaTd, colonnaTd, posizioneFinale, primaryKeyOpt, colonnaRdOpt)
    })

  private final val specifications = Specifications(recordsSettings
    .map(t => {
      val (sorgenteRd, tabellaTd, colonnaTd, posizioneFinale, primaryKeyOpt, colonnaRdOpt) = t
      specificationRecordApply(sorgenteRd, tabellaTd, colonnaTd, posizioneFinale, primaryKeyOpt, colonnaRdOpt)
    }))

  s"A ${className[Specifications]} object" should
    s"throw a ${className[MultipleSrcException]} if more than 1 source has been specified" in {

    val tryRwdActualTableName: Try[String] = Try {specifications.rwdActualTableName}
    assert(tryRwdActualTableName.isFailure)
    val exception = tryRwdActualTableName.asInstanceOf[Failure[_]].exception
    assert(exception.isInstanceOf[MultipleSrcException])
  }

  it should s"throw a ${className[MultipleDstException]} if more than 1 destination has been specified" in {

    val tryTrdActualTableName: Try[String] = Try {specifications.trdActualTableName}
    assert(tryTrdActualTableName.isFailure)
    val exception = tryTrdActualTableName.asInstanceOf[Failure[_]].exception
    assert(exception.isInstanceOf[MultipleDstException])
  }

  it should s"return a proper error condition column depending on underlying ${className[SpecificationRecord]}(s)" in {

    val expectedErrorConditionCol: Column = partialSpecificationRecords
      .filter(_.colonnaRdOpt.nonEmpty)
      .map(x => {
        val (colonnaTd, colonnaRdOpt) = (x.colonnaTd, x.colonnaRdOpt)
        val colonneRd: Seq[String] = colonnaRdOpt.get.split(", ")
        val firstErrorCondition = colonneRd
          .map(col(_).isNull)
          .reduce(_ || _)

        val secondErrorCondition = colonneRd
          .map(col(_).isNotNull)
          .reduce(_ && _) && col(colonnaTd).isNull

        firstErrorCondition || secondErrorCondition
      }).reduce(_ || _)

    assert(specifications.errorCondition == expectedErrorConditionCol)
  }

  it should s"return a proper set of primary key column(s) depending on underlying ${className[SpecificationRecord]}(s)" in {

    val expectedPrimaryKeyColumns: Seq[Column] = partialSpecificationRecords
      .filter(_.primaryKeyOpt.nonEmpty)
      .map(x => col(x.colonnaTd))

    assert(specifications.primaryKeyColumns == expectedPrimaryKeyColumns)
  }

  it should s"return columns ${ColumnName.RowIndex.name}, " +
    s"<each trdColumn sorted by its position>, " +
    s"${Seq(ColumnName.TsInserimento, ColumnName.DtInserimento, ColumnName.DtRiferimento)
      .map(_.name)
      .mkString(", ")} for trusted layer dataframes" in {

    val expectedTrdDfColumnSet: Seq[Column] = (col(ColumnName.RowIndex.name) :: Nil) ++
      partialSpecificationRecords
        .sortBy(_.posizioneFinale)
        .map(x => col(x.colonnaTd)) ++
      (col(ColumnName.TsInserimento.name) :: col(ColumnName.DtInserimento.name) :: col(ColumnName.DtRiferimento.name) :: Nil)

    assert(specifications.trdDfColumnSet == expectedTrdDfColumnSet)
  }

  it should s"return columns ${ColumnName.RowIndex.name}, " +
    s"<each rwdColumn sorted by its position>, " +
    s"${Seq(ColumnName.TsInserimento, ColumnName.DtInserimento, ColumnName.DtRiferimento)
      .map(_.name)
      .mkString(", ")} for raw layer dataframes" in {

    val expectedRwdDfColumnSet: Seq[Column] = (col(ColumnName.RowIndex.name) :: Nil) ++
      partialSpecificationRecords
        .filter(_.colonnaRdOpt.nonEmpty)
        .sortBy(_.posizioneFinale)
        .flatMap(_.colonnaRdOpt.get.split(", ").map(col))
        .distinct ++
      (col(ColumnName.TsInserimento.name) :: col(ColumnName.DtInserimento.name) :: col(ColumnName.DtRiferimento.name) :: Nil)

    assert(specifications.rwdDfColumnSet == expectedRwdDfColumnSet)
  }
}
