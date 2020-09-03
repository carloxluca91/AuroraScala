package it.carloni.luca.aurora.spark.exception

class MultipleSrcOrDstException(bancllName: String, sources: Seq[String], destinations: Seq[String])
  extends Exception(MultipleSrcOrDstException.msg
    .format(bancllName,
      sources.size,
      destinations.size,
      sources.map(x => s"'$x'").mkString(", "),
      destinations.map(x => s"'$x'").mkString(", ")))

object MultipleSrcOrDstException {

  private val msg: String = "Multiple sources (%s) or destinations (%s) specified for BANCLL '%s'. Sources : (%s), destinations: (%s)"
}
