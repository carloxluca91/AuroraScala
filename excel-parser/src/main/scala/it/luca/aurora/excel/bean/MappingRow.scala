package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import it.luca.aurora.excel.implicits._
import org.apache.poi.ss.usermodel.Row

case class MappingRow(sourceId: String,
                      inputPath: String,
                      inputFileFormat: String,
                      outputTableName: String)

object MappingRow extends ExcelRowDecoder[MappingRow] {

  override implicit def decode(row: Row): MappingRow = {

  MappingRow(sourceId = row(0).as[String],
    inputPath = row(1).as[String],
    inputFileFormat = row(2).as[String],
    outputTableName = row(3).as[String])
  }
}
