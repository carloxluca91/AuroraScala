package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import org.apache.poi.ss.usermodel.Row

case class SpecificationRow(sourceId: String,
                            rwDataSource: String,
                            trdDataSource: String,
                            rwColumn: String,
                            rwColumnType: String,
                            rwColumnPosition: Int,
                            flagDiscard: Boolean,
                            qualityCheck: Option[String],
                            transformation: Option[String],
                            trdColumn: Option[String],
                            trdColumnType: Option[String],
                            trdColumnPosition: Option[Int])

object SpecificationRow extends ExcelRowDecoder[SpecificationRow] {

  override implicit def decode(row: Row): SpecificationRow = {

    SpecificationRow(sourceId = row(0).as[String],
      rwDataSource = row(1).as[String],
      trdDataSource = row(2).as[String],
      rwColumn = row(3).as[String],
      rwColumnType = row(4).as[String],
      rwColumnPosition = row(5).as[Double].toInt,
      flagDiscard = row(6).asOption[String].exists(_.equalsIgnoreCase("y")),
      qualityCheck = row(7).asOption[String],
      transformation = row(8).asOption[String],
      trdColumn = row(9).asOption[String],
      trdColumnType = row(10).asOption[String],
      trdColumnPosition = row(11).asOption[Double].map(_.toInt))
  }
}