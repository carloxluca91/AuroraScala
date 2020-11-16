package it.luca.aurora.spark.exception

case class NoSpecificationException(bancllName: String)
  extends Exception(s"Unable to retrieve any specification for BANCLL '$bancllName'")