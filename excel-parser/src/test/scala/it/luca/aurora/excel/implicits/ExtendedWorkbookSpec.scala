package it.luca.aurora.excel.implicits

import it.luca.aurora.CustomSpec
import it.luca.aurora.excel.bean.Bean
import org.apache.poi.ss.usermodel.{Row, Workbook, WorkbookFactory}

import scala.collection.JavaConverters._

class ExtendedWorkbookSpec extends CustomSpec {

  private final val workbook: Workbook = WorkbookFactory
    .create(classOf[ExtendedCellSpec]
      .getClassLoader.getResourceAsStream("test.xlsx"))

  s"A ${className[ExtendedWorkbook]}" must "correctly decode a sheet into a Seq of beans" in {

    val rows: Seq[Row] = workbook.getSheetAt(0)
      .rowIterator().asScala.toSeq

    val beans: Seq[Bean] = workbook.as[Bean](0)
    assertResult(rows.size - 1) {
      beans.size
    }
  }
}
