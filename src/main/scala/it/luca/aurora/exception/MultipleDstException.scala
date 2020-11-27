package it.luca.aurora.exception

case class MultipleDstException(exceptionMsg: String)
  extends Throwable(exceptionMsg)

object MultipleDstException {

  def apply(bancllName: String, destinations: Seq[String]): MultipleDstException = {

    val dstStr = destinations.map(x => s"'$x'").mkString(", ")
    val msg = s"Multiple destinations (${destinations.size}) specified for BANCLL '$bancllName'. ($dstStr)"
    MultipleDstException(msg)
  }
}