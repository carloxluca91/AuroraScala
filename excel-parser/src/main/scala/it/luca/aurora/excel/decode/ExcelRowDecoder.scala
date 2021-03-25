package it.luca.aurora.excel.decode

import org.apache.poi.ss.usermodel.{Cell, CellType, Row}

trait ExcelRowDecoder[T <: Product with Serializable] {

  implicit class ExtendedCell(private val cell: Cell) {

    def as[R]: R = {

      val cellValue: Any = cell.getCellTypeEnum match {
        case CellType._NONE => null
        case CellType.NUMERIC => cell.getNumericCellValue
        case CellType.STRING => cell.getStringCellValue
        case CellType.FORMULA => null
        case CellType.BLANK => null
        case CellType.BOOLEAN => cell.getBooleanCellValue
        case CellType.ERROR => null
      }

      cellValue.asInstanceOf[R]
    }

    def asOption[R]: Option[R] = Option(as[R])
  }

  implicit class ExtendedRow(private val row: Row) {

    def apply(int: Int): Cell = row.getCell(int)
  }

  implicit def decode(row: Row): T
}
