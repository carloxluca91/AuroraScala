package it.luca.aurora.core

import org.slf4j.{Logger, LoggerFactory}

trait Logging {

  @transient
  protected lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

}
