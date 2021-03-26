package it.luca.aurora.logging

import org.apache.log4j.Logger

trait LazyLogging {

  @transient
  protected lazy val log: Logger = Logger.getLogger(getClass.getName)

}
