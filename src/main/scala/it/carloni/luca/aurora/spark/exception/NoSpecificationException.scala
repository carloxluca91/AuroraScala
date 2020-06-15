package it.carloni.luca.aurora.spark.exception

class NoSpecificationException(bancllName: String)
  extends Exception(NoSpecificationException.msg.format(bancllName))

object NoSpecificationException {

  private val msg: String = "Unable to retrieve any specification for BANCLL %s"
}
