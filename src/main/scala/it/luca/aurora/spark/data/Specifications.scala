package it.luca.aurora.spark.data

import it.luca.aurora.exception.{MultipleDstException, MultipleSrcException, UnexistingLookupException}
import it.luca.aurora.spark.functions.common.ColumnExpressionParser
import it.luca.aurora.utils.ColumnName
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions.{col, lower, trim, when}

case class Specifications(private val specificationRecords: Seq[SpecificationRecord]) {

  private final val logger = Logger.getLogger(getClass)
  private final val columnsFromSpecifications: (Seq[SpecificationRecord] => Seq[Column]) => Seq[Column] =
    op => {

    val rowIdCol = col(ColumnName.RowId.name)
    val tsInserimentoCol = col(ColumnName.TsInserimento.name)
    val dtInserimentoCol = col(ColumnName.DtInserimento.name)
    val dtRiferimentoCol = col(ColumnName.DtRiferimento.name)

    (rowIdCol :: Nil) ++ op(specificationRecords) ++ (tsInserimentoCol :: dtInserimentoCol :: dtRiferimentoCol :: Nil)
  }

  def errorCondition: Column = {

    specificationRecords
      .map(_.errorCondition)
      .filter(_.nonEmpty)
      .map(_.get)
      .reduce(_ || _)
  }

  def errorDescriptions: Seq[(String, Column)] = {

    specificationRecords
      .filter(_.errorDescription.nonEmpty)
      .sortBy(_.posizioneFinale)
      .map(_.errorDescription.get)
  }

  def nonEmpty: Boolean = specificationRecords.nonEmpty

  def length: Int = specificationRecords.length

  def lookupTypesAndIds: Seq[(String, String)] = {

    specificationRecords
      .filter(_.isLookupFlagOn)
      .map(x => (x.tipoLookup.get.toLowerCase, x.lookupId.get.toLowerCase))
      .distinct
  }

  def primaryKeyColumns: Seq[Column] = {

    specificationRecords
      .filter(_.flagPrimaryKey.nonEmpty)
      .map(x => col(x.colonnaTd))
  }

  def rwdActualTableName: String = {

    val srcTables: Seq[String] = specificationRecords
      .map(_.sorgenteRd)
      .distinct

    if (srcTables.length == 1) {
      srcTables.head
    } else {

      val bancllName: String = specificationRecords.map(_.flusso).head
      throw MultipleSrcException(bancllName, srcTables)
    }
  }

  def rwdDfColumnSet: Seq[Column] = {

    val op: Seq[SpecificationRecord] => Seq[Column] =
      records => records
        .filter(_.inputRdColumns.nonEmpty)
        .sortBy(_.posizioneFinale)
        .flatMap(_.inputRdColumns.get)
        .distinct
        .map(col)

    columnsFromSpecifications(op)
  }
  
  def trdActualTableName: String = {

    val dstTables: Seq[String] = specificationRecords
      .map(_.tabellaTd)
      .distinct

    if (dstTables.length == 1) {
      dstTables.head
    } else {

      val bancllName: String = specificationRecords.map(_.flusso).head
      throw MultipleDstException(bancllName, dstTables)
    }
  }

  def trdColumns(lookupDf: DataFrame): Seq[(String, Column)] = {

    specificationRecords
      .sortBy(_.posizioneFinale)
      .map(s => {

        logger.info(s"Analyzing specifications related to trusted column '${s.colonnaTd}'")
        val trustedColumnBeforeLookup: Column = s.funzioneEtl match {
          case None =>

            // If an ETL expression has not been defined, get simple raw column
            logger.info(s"No ETL function to apply to input column '${s.colonnaRd.get}'")
            col(s.colonnaRd.get)

          case Some(etlFunction) =>

            // Otherwise, parse provided ETL function
            logger.info(s"Detected an ETL function to apply: '$etlFunction'")
            val etlTransformedColumn: Column = ColumnExpressionParser(etlFunction)
            logger.info(s"Successfully parsed ETL function '$etlFunction")
            etlTransformedColumn

        }

        // If a lookup operation is specified
        val trustedColumnAfterLookup: Column = if (s.isLookupFlagOn) {

          // Try to retrieve related cases
          val lookupType = s.tipoLookup.get.toLowerCase()
          val lookupId = s.tipoLookup.get.toLowerCase
          val lookupDfFilterCondition: Column = trim(lower(col(ColumnName.LookupTipo.name))) === lookupType &&
            trim(lower(col(ColumnName.LookupId.name))) === lookupId

          val lookupCases: Seq[Row] = lookupDf
            .filter(lookupDfFilterCondition)
            .select(ColumnName.LookupValoreOriginale.name, ColumnName.LookupValoreSostituzione.name)
            .collect()
            .toSeq

          if (lookupCases.nonEmpty) {

            // If some cases are retrieved, prepare a suitable case-when statement
            logger.info(s"Identified ${lookupCases.size} case(s) for lookup type '$lookupType', id '$lookupId'")
            val firstCaseColumn: Column = when(trustedColumnBeforeLookup === lookupCases.head.get(0), lookupCases.head.get(1))
            lookupCases.tail
              .foldLeft(firstCaseColumn)((column, row) =>
                column.when(trustedColumnBeforeLookup === row.get(0), row.get(1)))

          } else throw UnexistingLookupException(s.colonnaTd, lookupType, lookupId)

          // Otherwise, get simple column expression after ETL
        } else trustedColumnBeforeLookup

        (s.colonnaTd, trustedColumnAfterLookup)
      })
  }

  def trdDfColumnSet: Seq[Column] = {

    val op: Seq[SpecificationRecord] => Seq[Column] =
      records => records
        .sortBy(_.posizioneFinale)
        .map(x => col(x.colonnaTd))

    columnsFromSpecifications(op)
  }
}
