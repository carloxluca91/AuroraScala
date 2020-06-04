package it.carloni.luca.aurora.spark.exceptions

class MultipleRdSourceException(bancllName: String, multipleRawSources: List[String])
  extends Exception(MultipleRdSourceException.msg
    .format(bancllName, multipleRawSources.mkString(", ")))

object MultipleRdSourceException {

  private val msg: String = "Multiple rd sources specified for BANCLL %s (%s)"
}
