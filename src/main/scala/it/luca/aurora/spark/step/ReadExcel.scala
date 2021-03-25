package it.luca.aurora.spark.step

import it.luca.aurora.enumeration.JobVariable
import it.luca.aurora.excel.ExcelReader
import org.apache.poi.ss.usermodel.Workbook

case class ReadExcel(override protected val input: String)
  extends IOStep[String, Workbook](input, "READ_EXCEL") {

  override def run(): (JobVariable.Value, Workbook) = (JobVariable.ExcelWorkbook, ExcelReader.read(input))
}
