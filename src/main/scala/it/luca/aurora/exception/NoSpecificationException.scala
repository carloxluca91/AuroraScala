package it.luca.aurora.exception

case class NoSpecificationException(bancllName: String)
  extends Throwable(s"Unable to retrieve any specification for BANCLL '$bancllName'")