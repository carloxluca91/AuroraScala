package it.luca.aurora.exception

case class NoSpecificationException(sourceId: String)
  extends Throwable(s"Unable to retrieve any specification for '$sourceId'")