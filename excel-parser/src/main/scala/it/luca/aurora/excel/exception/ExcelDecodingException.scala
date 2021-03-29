package it.luca.aurora.excel.exception

import org.apache.poi.ss.usermodel.Cell

case class ExcelDecodingException(message: String)
  extends Throwable(message)

object ExcelDecodingException {

  def apply[T](cell: Cell, tClass: Class[T]): ExcelDecodingException = {

    val msg = s"Error on decoding cell [${cell.getRowIndex}, ${cell.getColumnIndex}]. " +
      s"Requested cell content as type ${tClass.getName}, while actual content has type ${cell.getCellTypeEnum}"
    ExcelDecodingException(msg)
  }
}
