package it.luca.aurora.excel.implicits

import it.luca.aurora.excel.BaseSpec
import org.apache.poi.ss.usermodel.{Cell, CellType}

class ExtendedCellSpec extends BaseSpec {

  private final val cell: Cell = mock[Cell]
  private final val expectedString = "STRING"
  private final val expectedDouble = 3.14
  private final val expectedBoolean = false

  s"An ${clazz[ExtendedCell]}" must "correctly return typed content" in {

    // String
    (cell.getStringCellValue: () => String).expects().returning(expectedString)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.STRING)
    assertResult(expectedString)(cell.as[String])

    /*
    // Numeric
    (cell.getNumericCellValue: () => Double).expects().returning(expectedDouble)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC)
    assertResult(expectedDouble)(cell.as[Double])

    // Boolean
    (cell.getBooleanCellValue: () => Boolean).expects().returning(expectedBoolean)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.BOOLEAN)
    assertResult(expectedBoolean)(cell.as[Boolean])

     */
  }

  it must "correctly return optional typed content" in {

    def assertOpt[T](value: T): Unit = {

      // First call (expecting Some(T))
      val maybeT: Option[T] = cell.asOption[T]
      assert(maybeT.nonEmpty && maybeT.get.equals(value))

      // Second call (expecting None)
      assert(cell.asOption[T].isEmpty)
    }

    // String
    (cell.getStringCellValue: () => String).expects().returning(expectedString)
    (cell.getStringCellValue: () => String).expects().returning(null)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.STRING).noMoreThanTwice()
    assertOpt[String](expectedString)

    /*
    // Numeric
    (cell.getNumericCellValue: () => java.lang.Double).expects().returning(expectedDouble)
    (cell.getNumericCellValue: () => java.lang.Double).expects().returning(null)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.NUMERIC).noMoreThanTwice()
    assertOpt[Double](expectedDouble)

    // Boolean
    (cell.getBooleanCellValue: () => java.lang.Boolean).expects().returning(expectedBoolean)
    (cell.getBooleanCellValue: () => java.lang.Boolean).expects().returning(null)
    (cell.getCellTypeEnum: () => CellType).expects().returning(CellType.BOOLEAN).noMoreThanTwice()
    assertOpt[Boolean](expectedBoolean)

     */
  }
}
