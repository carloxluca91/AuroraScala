package it.luca.aurora.excel

import org.apache.poi.ss.usermodel.Row

case class ExcelRow(sourceId: String,
                    rwDataSource: String,
                    trdDataSource: String,
                    rwColumn: String,
                    rwColumnType: String,
                    rwColumnPosition: Int,
                    flagDiscard: Option[Boolean],
                    qualityCheck: Option[String],
                    transformation: Option[String],
                    trdColumn: Option[String],
                    trdColumnType: Option[String],
                    trdColumnPosition: Option[String])

object ExcelRow {

  def apply(row: Row): Unit = {

  }
}