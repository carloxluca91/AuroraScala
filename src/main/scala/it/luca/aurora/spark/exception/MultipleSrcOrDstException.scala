package it.luca.aurora.spark.exception

case class MultipleSrcOrDstException(exceptionMsg: String)
  extends Exception(exceptionMsg)

object MultipleSrcOrDstException {

  def apply(bancllName: String, sources: Seq[String], destinations: Seq[String]): MultipleSrcOrDstException = {

    val srcStr = sources.map(x => s"'$x'").mkString(", ")
    val dstStr = destinations.map(x => s"'$x'").mkString(", ")
    val msg = s"Multiple sources (${sources.size}) or destinations (${destinations.size}) specified for BANCLL '$bancllName'. " +
      s"Sources : ($srcStr), destinations: ($dstStr)"

    MultipleSrcOrDstException(msg)

  }
}
