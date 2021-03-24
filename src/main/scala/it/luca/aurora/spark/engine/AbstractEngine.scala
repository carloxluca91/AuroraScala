package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.sql.SQLContext

abstract class AbstractEngine(protected val sqlContext: SQLContext,
                              protected val propertiesFile: String)
  extends Logging {

  protected final val jobProperties = new PropertiesConfiguration(propertiesFile)

}