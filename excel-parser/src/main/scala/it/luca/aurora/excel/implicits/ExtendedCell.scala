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
      case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
    }).asInstanceOf[T]
  }

  def as[T, R](cast: T => R)(implicit tTypeTag: TypeTag[T]): R = cast(as[T])

  def asOption[T](implicit tTypeTag: TypeTag[T]): Option[T] = {

    val value = cell.getCellTypeEnum match {
      case CellType.NUMERIC => cell.getNumericCellValue
      case CellType.STRING => cell.getStringCellValue
      case CellType.BLANK => null
      case _ => throw ExcelDecodingException(cell, typeOf[T].getClass)
    }

    Option(value).map(_.asInstanceOf[T])
  }

  def asOption[T, R](cast: T => R)(implicit tTypeTag: TypeTag[T]): Option[R] = asOption[T].map(cast)

  def asString: String = {

    cell.getCellTypeEnum match {
      case CellType.STRING => cell.getStringCellValue
      case _ => throw ExcelDecodingException(cell, classOf[String])
    }
  }

  def asStringOpt: Option[String] = {

    cell.getCellTypeEnum match {
      case CellType.STRING | CellType.BLANK => Option(cell.getStringCellValue)
      case _ => throw ExcelDecodingException(cell, classOf[Option[String]])
    }
  }

  def asInt: Int = {

    cell.getCellTypeEnum match {
      case CellType.NUMERIC => cell.getNumericCellValue.toInt
      case _ => throw ExcelDecodingException(cell, classOf[Int])
    }
  }

  def asIntOpt: Option[Int] = {

    cell.getCellTypeEnum match {
      case CellType.NUMERIC | CellType.BLANK => Option(cell.getNumericCellValue).map(_.toInt)
      case _ => throw ExcelDecodingException(cell, classOf[Option[String]])
    }
  }
}