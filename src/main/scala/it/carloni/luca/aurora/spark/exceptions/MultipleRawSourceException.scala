package it.carloni.luca.aurora.spark.exceptions

class MultipleRawSourceException(bancllName: String, multipleRawSources: Seq[String])
  extends Exception(MultipleRawSourceException.msg
    .format(bancllName, multipleRawSources.mkString(", ")))

object MultipleRawSourceException {

  val msg: String = "Multiple raw sources specified for BANCLL %s (%s)"
}
