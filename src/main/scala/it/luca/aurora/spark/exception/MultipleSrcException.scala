package it.luca.aurora.spark.exception

case class MultipleSrcException(exceptionMsg: String)
  extends Exception(exceptionMsg)

object MultipleSrcException {

  def apply(bancllName: String, sources: Seq[String]): MultipleDstException = {

    val srcStr = sources.map(x => s"'$x'").mkString(", ")
    val msg = s"Multiple sources (${sources.size}) specified for BANCLL '$bancllName' ($srcStr)"
    MultipleDstException(msg)
  }
}
