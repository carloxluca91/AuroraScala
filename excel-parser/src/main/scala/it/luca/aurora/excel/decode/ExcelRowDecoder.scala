package it.luca.aurora.excel.decode

import org.apache.poi.ss.usermodel.Row

trait ExcelRowDecoder[T <: Product with Serializable] {

  implicit def decode(row: Row): T
}
