package it.luca.aurora.excel.exception

import it.luca.aurora.excel.bean.SpecificationRow

case class UndefinedTrdColumnException(msg: String)
  extends Throwable(msg)

object UndefinedTrdColumnException {

  def apply(row: SpecificationRow): UndefinedTrdColumnException = {

    val msg = s"Invalid specification for raw column ${row.rwColumn}. " +
      s"Flag discard is ${row.flagDiscard}, but related trusted column is not defined"
    UndefinedTrdColumnException(msg)
  }
}
