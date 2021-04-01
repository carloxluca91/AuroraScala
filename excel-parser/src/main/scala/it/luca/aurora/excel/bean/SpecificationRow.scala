package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import it.luca.aurora.excel.implicits._
import org.apache.poi.ss.usermodel.Row

case class SpecificationRow(dataSource: String,
                            rwTable: String,
                            trdTable: String,
                            rwColumn: String,
                            rwColumnType: String,
                            rwColumnPosition: Int,
                            rwColumnDescription: Option[String],
                            inputCheck: Option[String],
                            inputTransformation: Option[String],
                            trdColumn: Option[String],
                            trdColumnType: Option[String],
                            trdColumnPosition: Option[Int],
                            primaryKey: Boolean)

object SpecificationRow extends ExcelRowDecoder[SpecificationRow] {

  override implicit def decode(row: Row): SpecificationRow = {

    SpecificationRow(dataSource = row(0).as[String],
      rwTable = row(1).as[String],
      trdTable = row(2).as[String],
      rwColumn = row(3).as[String],
      rwColumnType = row(4).as[String],
      rwColumnPosition = row(5).as[Double, Int](d => d.toInt),
      rwColumnDescription = row(6).asOption[String],
      inputCheck = row(7).asOption[String],
      inputTransformation = row(8).asOption[String],
      trdColumn = row(9).asOption[String],
      trdColumnType = row(10).asOption[String],
      trdColumnPosition = row(11).asOption[Double, Int](d => d.toInt),
      primaryKey = row(12).asOption[String].exists(_.equalsIgnoreCase("y"))
    )
  }
}