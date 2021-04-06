package it.luca.aurora.excel.bean

import it.luca.aurora.core.Logging
import it.luca.aurora.core.utils.classSimpleName
import it.luca.aurora.excel.exception.UndefinedTrdColumnException
import it.luca.aurora.spark.sql.parser.SqlParser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class SpecificationRows(private val specifications: Seq[SpecificationRow]) {

  private val inputCheckTriples: Seq[(String, Column, String)] = specifications.collect {
    case s: SpecificationRow if s.inputCheck.nonEmpty =>
      (s.inputCheck.get, SqlParser.parse(s.inputCheck.get), s"${s.rwColumn}_input_check")
  }

  def inputCheckColumns(): Seq[Column] = inputCheckTriples.map(_._2)

  def inputChecks(): Seq[String] = inputCheckTriples.map(_._1)

  val rwColumns: Seq[Column] = specifications
    .sortBy(_.rwColumnPosition)
    .map(x => col(x.rwColumn))

  val sourceId: String = specifications.head.dataSource

  val trdColumns: Seq[(Int, String, Column)] = {
    specifications.collect {
      case s: SpecificationRow if s.trdColumn.nonEmpty =>

        val position = s.trdColumnPosition.get
        val columnName = s.trdColumn.get
        val column = s.inputTransformation
          .map(SqlParser.parse)
          .getOrElse(col(s.rwColumn).as(s.trdColumn.get))
        (position, columnName, column)
    }
  }
}

object SpecificationRows extends Logging {

  def from(specifications: Seq[SpecificationRow]): SpecificationRows = {

    // Trd columns fully defined (i.e. when trdColumn is not empty, trdColumnType and trdColumnPosition must be as well)
    val trdColumnsNotFullyDefined = specifications.filter(_.trdColumn.nonEmpty)
      .filter(x => x.trdColumnType.isEmpty || x.trdColumnPosition.isEmpty)
    if (trdColumnsNotFullyDefined.nonEmpty) {
      throw UndefinedTrdColumnException(trdColumnsNotFullyDefined)
    }

    log.info(s"Validated all of ${specifications.size} ${classSimpleName[SpecificationRow]}(s)")
    SpecificationRows(specifications)
  }
}
