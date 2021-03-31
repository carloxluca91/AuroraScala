package it.luca.aurora.excel.exception

case class MultipleRwTableException(exceptionMsg: String)
  extends Throwable(exceptionMsg)

object MultipleRwTableException {

  def apply(dataSource: String, tables: Seq[String]): MultipleRwTableException = {

    val msg = s"Multiple raw tables for dataSource '$dataSource' (${tables.mkString(", ")})"
    MultipleRwTableException(msg)
  }
}
