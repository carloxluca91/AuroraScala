package it.luca.aurora.excel

import grizzled.slf4j.Logging
import org.apache.poi.ss.usermodel.{Row, Workbook}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

package object implicits extends Logging {

  implicit class ExtendedWorkbook(private val workBook: Workbook) {

    def as[T](sheetIndex: Int, skipHeader: Boolean)(implicit rowDecoder: Row => T): Seq[T] = {

      val rowIterator: java.util.Iterator[Row] = workBook.getSheetAt(sheetIndex).rowIterator()
      if (skipHeader) rowIterator.next()
      rowIterator.asScala.toSeq.map { r =>
        Try {rowDecoder(r)} match {
          case Success(value) => value
          case Failure(exception) =>
            error(s"Error while converting row # ${r.getRowNum}. Stack trace: ", exception)
            throw exception
        }
      }
    }
  }
}
