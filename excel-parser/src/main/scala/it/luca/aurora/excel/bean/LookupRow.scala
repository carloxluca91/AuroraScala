package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import it.luca.aurora.excel.implicits._
import org.apache.poi.ss.usermodel.Row

case class LookupRow(dataSource: String,
                     column: String,
                     originalValue: String,
                     substitution: String)

object LookupRow extends ExcelRowDecoder[LookupRow] {

  override implicit def decode(row: Row): LookupRow = {

    LookupRow(dataSource = row(0).as[String],
      column = row(1).as[String],
      originalValue = row(2).as[String],
      substitution = row(3).as[String])
  }
}
