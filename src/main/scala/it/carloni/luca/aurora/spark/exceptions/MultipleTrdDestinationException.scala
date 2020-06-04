package it.carloni.luca.aurora.spark.exceptions

class MultipleTrdDestinationException(bancllName: String, trdDestinations: List[String])
  extends Exception(MultipleTrdDestinationException.msg
    .format(bancllName, trdDestinations.mkString(", ")))

object MultipleTrdDestinationException {

  private val msg: String = "Multiple trd destinations specified for BANCLL %s (%s)"
}