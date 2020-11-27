package it.luca.aurora.exception

case class MultipleSrcException(exceptionMsg: String)
  extends Throwable(exceptionMsg)

object MultipleSrcException {

  def apply(bancllName: String, sources: Seq[String]): MultipleSrcException = {

    val srcStr = sources.map(x => s"'$x'").mkString(", ")
    val msg = s"Multiple sources (${sources.size}) specified for BANCLL '$bancllName'. ($srcStr)"
    MultipleSrcException(msg)
  }
}
