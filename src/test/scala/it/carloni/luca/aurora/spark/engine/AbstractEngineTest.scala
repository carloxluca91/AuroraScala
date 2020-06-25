package it.carloni.luca.aurora.spark.engine

import java.io.File

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import scala.xml.{Elem, NodeSeq, XML}

class AbstractEngineTest extends org.scalatest.FunSuite {

  private final val logger = Logger.getLogger(getClass)

  test("Testing table schema parsing") {

    val jobProperties: PropertiesConfiguration = new PropertiesConfiguration
    jobProperties.load(getClass.getClassLoader.getResourceAsStream("spark_job.properties"))
    val schemaFilePath: String = jobProperties.getString("table.mapping_specification.xml.schema.path");
    val schemaFile: File = new File(schemaFilePath)

    assert(schemaFile.exists())

    val mappingSpecificationXML: Elem = XML.loadFile(schemaFile)
    val columnsNodeSeq: NodeSeq = mappingSpecificationXML \\ "tableSchema" \\ "columns" \\ "column"

    assert(columnsNodeSeq.length == 18)
  }
}
