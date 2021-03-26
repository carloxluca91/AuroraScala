package it.luca.aurora.excel.implicits

import org.apache.poi.ss.usermodel.{Cell, CellType}

class ExtendedCell(private val cell: Cell) {

  def as[R]: R = {

    (cell.getCellTypeEnum match {
    case CellType.NUMERIC => cell.getNumericCellValue
    case CellType.STRING => cell.getStringCellValue
    case CellType.BOOLEAN => cell.getBooleanCellValue
    case _ => null
    }).asInstanceOf[R]
  }

  def asOption[R]: Option[R] = Option(as[R])
}