package it.luca.aurora.excel

import org.apache.poi.ss.usermodel.{Cell, Row, Workbook}

package object implicits {

  implicit def toExtendedWorkBook(workbook: Workbook): ExtendedWorkbook = new ExtendedWorkbook(workbook)

  implicit def toExtendedCell(cell: Cell): ExtendedCell = new ExtendedCell(cell)

  implicit def toExtendedRow(row: Row): ExtendedRow = new ExtendedRow(row)

}
