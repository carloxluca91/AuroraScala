package it.luca.aurora.spark.step

import it.luca.aurora.enumeration.JobVariable
import it.luca.aurora.excel.implicits._
import org.apache.poi.ss.usermodel.{Row, Workbook}

case class DecodeExcelSheet[T](override protected val input: Workbook,
                               private val sheetIndex: Int,
                               private val skipHeader: Boolean)(private implicit val decodeRow: Row => T)
  extends IOStep[Workbook, Seq[T]](input, s"DECODE_EXCEL_SHEET_$sheetIndex") {

  override def run(): (JobVariable.Value, Seq[T]) = (JobVariable.ExcelDecodedBeans, input.as[T](sheetIndex, skipHeader))
}
