package it.luca.aurora.excel.bean

import it.luca.aurora.excel.exception.{MultipleRwTableException, MultipleTrdTableException, UndefinedTrdColumnException}
import it.luca.aurora.logging.Logging
import it.luca.aurora.spark.sql.parser.SqlParser
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class SpecificationRows(private val specifications: Seq[SpecificationRow])
  extends Logging {

  validate()

  private val inputCheckTriples: Seq[(String, Column, String)] = specifications.collect {
    case s: SpecificationRow if s.inputCheck.nonEmpty =>
      (s.inputCheck.get, SqlParser.parse(s.inputCheck.get), s"${s.rwColumn}_input_check")
  }

  private def validate(): Unit = {

    // Input rw tables
    val rwDataSources: Seq[String] = specifications.map(_.rwTable).distinct
    if (rwDataSources.size > 1) {
      throw MultipleRwTableException(sourceId, rwDataSources)
    }

    // Output trd tables
    val trdDataSources: Seq[String] = specifications.map(_.trdTable).distinct
    if (trdDataSources.size > 1) {
      throw MultipleTrdTableException(sourceId, trdDataSources)
    }

    // Trd columns fully defined (i.e. when trdColumn is not empty, trdColumnType and trdColumnPosition must be as well
    val trdColumnsNotFullyDefined = specifications.filter(_.trdColumn.nonEmpty)
      .filter(x => x.trdColumnType.isEmpty || x.trdColumnPosition.isEmpty)
    if (trdColumnsNotFullyDefined.nonEmpty) {
      throw UndefinedTrdColumnException(trdColumnsNotFullyDefined)
    }

    log.info(s"Validated all of ${specifications.size} ${classOf[SpecificationRow].getSimpleName}(s)")
  }

  def inputCheckColumns(): Seq[Column] = inputCheckTriples.map(_._2)

  def inputChecks(): Seq[String] = inputCheckTriples.map(_._1)

  val rwColumns: Seq[Column] = specifications
    .sortBy(_.rwColumnPosition)
    .map(x => col(x.rwColumn))

  val sourceId: String = specifications.head.dataSource

  val rwDataSource: String = specifications.head.rwTable

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

  val trdDataSource: String = specifications.head.trdTable
}
