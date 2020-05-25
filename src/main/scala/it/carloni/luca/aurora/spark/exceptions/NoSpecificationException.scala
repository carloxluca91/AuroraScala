package it.carloni.luca.aurora.spark.exceptions

class NoSpecificationException(bancllName: String)
  extends Exception(NoSpecificationException.msg.format(bancllName))

object NoSpecificationException {

  val msg: String = "Unable to retrieve any specification for BANCLL %s"
}
