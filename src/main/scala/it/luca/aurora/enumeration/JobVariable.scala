package it.luca.aurora.enumeration

object JobVariable extends Enumeration {

  protected case class Val(name: String) extends super.Val

  implicit def toVal(x: Value): Val = x.asInstanceOf[Val]

  val ExcelWorkbook: Val = Val("EXCEL_WORKBOOK")
  val ExcelDecodedBeans: Val = Val("EXCEL_DECODED_BEANS")
  val SpecificationDf: Val = Val("SPECIFICATION_DF")
}
