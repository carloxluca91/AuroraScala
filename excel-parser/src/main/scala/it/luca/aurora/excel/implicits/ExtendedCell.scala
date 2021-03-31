package it.luca.aurora.excel.implicits

import it.luca.aurora.excel.exception.ExcelDecodingException
import org.apache.poi.ss.usermodel.{Cell, CellType}

import scala.reflect.runtime.universe._

class ExtendedCell(private val cell: Cell) {

  def as[T](implicit tTypeTag: TypeTag[T]): T = {

    val tType = typeOf[T]
    (cell.getCellTypeEnum match {
      case CellType.NUMERIC if tType.equals(typeOf[Double]) => cell.getNumericCellValue
      case CellType.STRING if tType.equals(typeOf[String]) => cell.getStringCellValue
      case CellType.FORMULA => cell.getCachedFormulaResultTypeEnum match {
        case CellType.NUMERIC if tType.equals(typeOf[Double]) => cell.getNumericCellValue
        case CellType.STRING if tType.equals(typeOf[String]) => cell.getStringCellValue
        case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
      }
      case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
    }).asInstanceOf[T]
  }

  def as[T, R](cast: T => R)(implicit tTypeTag: TypeTag[T]): R = cast(as[T])

  def asOption[T](implicit tTypeTag: TypeTag[T]): Option[T] = {

    val tType = typeOf[T]
    val value = cell.getCellTypeEnum match {
      case CellType.NUMERIC if tType.equals(typeOf[Double]) => cell.getNumericCellValue
      case CellType.STRING if tType.equals(typeOf[String]) => cell.getStringCellValue
      case CellType.FORMULA => cell.getCachedFormulaResultTypeEnum match {
        case CellType.NUMERIC if tType.equals(typeOf[Double]) => cell.getNumericCellValue
        case CellType.STRING if tType.equals(typeOf[String]) => cell.getStringCellValue
        case CellType.BLANK => null
        case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
      }
      case CellType.BLANK => null
      case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
    }

    Option(value).map(_.asInstanceOf[T])
  }

  def asOption[T, R](cast: T => R)(implicit tTypeTag: TypeTag[T]): Option[R] = asOption[T].map(cast)
}