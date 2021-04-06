package it.luca.aurora.exception

import it.luca.aurora.spark.bean.SpecificationRow

case class UndefinedTrdColumnException(msg: String)
  extends Throwable(msg)

object UndefinedTrdColumnException {

  def apply(specifications: Seq[SpecificationRow]): UndefinedTrdColumnException = {

    val msg = s"Invalid specification for raw column(s) ${specifications.map(_.rwColumn).mkString(", ")}. " +
      s"Trusted columns are defined, but related trusted position or type are not"
    UndefinedTrdColumnException(msg)
  }
}
