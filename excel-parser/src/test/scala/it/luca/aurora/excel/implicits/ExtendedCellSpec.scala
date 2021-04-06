package it.luca.aurora.excel.implicits

import it.luca.aurora.CustomSpec
import it.luca.aurora.excel.exception.ExcelDecodingException
import org.apache.poi.ss.usermodel.{Cell, CellType}
import org.scalamock.scalatest.MockFactory

class ExtendedCellSpec extends CustomSpec with MockFactory {

  private final val mockCell = mock[Cell]
  private final val expectedString = "STRING"
  private final val expectedDouble = 3.14

  s"An ${className[ExtendedCell]}" must s"return typed content" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.STRING)
    (mockCell.getStringCellValue: () => String).expects().returning(expectedString)
    assertResult(expectedString) {
      mockCell.as[String]
    }

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC)
    (mockCell.getNumericCellValue: () => Double).expects().returning(expectedDouble)
    assertResult(expectedDouble) {
      mockCell.as[Double]
    }
  }

  it must "return typed-converted content" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC)
    (mockCell.getNumericCellValue: () => Double).expects().returning(expectedDouble)
    val output = mockCell.as[Double, Int](d => d.toInt)
    assert(output.isInstanceOf[Int])
    assertResult(expectedDouble.toInt) {
      output
    }
  }

  it must s"throw a ${className[ExcelDecodingException]} when requesting wrong data type" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC).noMoreThanTwice()
    (mockCell.getRowIndex: () => Int).expects().returning(0)
    (mockCell.getColumnIndex: () => Int).expects().returning(0)
    assertThrows[ExcelDecodingException]{
      mockCell.as[String]
    }
  }

  it must "return a typed and non-empty Option" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.STRING)
    (mockCell.getStringCellValue: () => String).expects().returning(expectedString)
    val option = mockCell.asOption[String]
    assert(option.nonEmpty)
    assertResult(expectedString) {
      option.get
    }
  }

  it must "return a typed-converted and non-empty Option" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC)
    (mockCell.getNumericCellValue: () => Double).expects().returning(expectedDouble)
    val option = mockCell.asOption[Double, Int](d => d.toInt)
    assert(option.nonEmpty)
    val optionValue = option.get
    assert(optionValue.isInstanceOf[Int])
    assertResult(expectedDouble.toInt) {
      optionValue
    }
  }

  it must "return an empty Option if requested content is null" in {

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.STRING)
    (mockCell.getStringCellValue: () => String).expects().returning(null)
    assert(mockCell.asOption[String].isEmpty)

    (mockCell.getCellTypeEnum: () => CellType).expects().returning(CellType.BLANK)
    assert(mockCell.asOption[String].isEmpty)
  }
}
