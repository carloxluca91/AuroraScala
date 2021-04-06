package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import it.luca.aurora.excel.implicits._
import org.apache.poi.ss.usermodel.Row

case class SpecificationRow(dataSource: String,
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
      rwColumn = row(1).as[String],
      rwColumnType = row(2).as[String],
      rwColumnPosition = row(3).as[Double, Int](d => d.toInt),
      rwColumnDescription = row(4).asOption[String],
      inputCheck = row(5).asOption[String],
      inputTransformation = row(6).asOption[String],
      trdColumn = row(7).asOption[String],
      trdColumnType = row(8).asOption[String],
      trdColumnPosition = row(9).asOption[Double, Int](d => d.toInt),
      primaryKey = row(10).asOption[String].exists(_.equalsIgnoreCase("y"))
    )
  }
}