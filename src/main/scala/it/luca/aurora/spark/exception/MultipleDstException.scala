package it.luca.aurora.spark.exception

case class MultipleDstException(exceptionMsg: String)
  extends Exception(exceptionMsg)

object MultipleDstException {

  def apply(bancllName: String, destinations: Seq[String]): MultipleDstException = {

    val srcStr = destinations.map(x => s"'$x'").mkString(", ")
    val msg = s"Multiple destinations (${destinations.size}) specified for BANCLL '$bancllName' ($srcStr)"
    MultipleDstException(msg)
  }
}
