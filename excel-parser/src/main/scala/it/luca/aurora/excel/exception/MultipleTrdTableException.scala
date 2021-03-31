package it.luca.aurora.excel.exception

case class MultipleTrdTableException(exceptionMsg: String)
  extends Throwable(exceptionMsg)

object MultipleTrdTableException {

  def apply(dataSource: String, tables: Seq[String]): MultipleTrdTableException = {

    val msg = s"Multiple trusted tables for dataSource '$dataSource' (${tables.mkString(", ")})"
    MultipleTrdTableException(msg)
  }
}