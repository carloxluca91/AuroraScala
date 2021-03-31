package it.luca.aurora.excel.bean

import it.luca.aurora.excel.exception.{MultipleRwTableException, MultipleTrdTableException, UndefinedTrdColumnException}
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

  val rwColumns: Seq[Column] = specifications.map(x => col(x.rwColumn))

  val sourceId: String = specifications.head.dataSource

  val rwDataSource: String = {
    val rwDataSources: Seq[String] = specifications.map(_.rwTable).distinct
    if (rwDataSources.size > 1) {
      throw MultipleRwTableException(sourceId, rwDataSources)
    } else {
      rwDataSources.head
    }
  }

  val trdColumns: Seq[(String, Column)] = {
    specifications.collect {
      case s: SpecificationRow if !s.flagDiscard && s.trdColumn.nonEmpty =>
        (s.trdColumn.get, SqlParser.parse(s.inputTransformation.get))
      case s: SpecificationRow if !s.flagDiscard && s.trdColumn.isEmpty =>
        throw UndefinedTrdColumnException(s)
    }
  }

  val trdDataSource: String = {
    val trdDataSources: Seq[String] = specifications.map(_.trdTable).distinct
    if (trdDataSources.size > 1) {
      throw MultipleTrdTableException(sourceId, trdDataSources)
    } else {
      trdDataSources.head
    }
  }

}
