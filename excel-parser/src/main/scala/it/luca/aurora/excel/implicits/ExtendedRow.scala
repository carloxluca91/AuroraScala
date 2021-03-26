package it.luca.aurora.excel.implicits

import org.apache.poi.ss.usermodel.{Cell, Row}

class ExtendedRow(private val row: Row) {

  def apply(int: Int): Cell = row.getCell(int)
}
