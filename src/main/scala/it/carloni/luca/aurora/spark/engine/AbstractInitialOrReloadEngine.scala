package it.carloni.luca.aurora.spark.engine

import java.io.{File, FileNotFoundException}

import it.carloni.luca.aurora.utils.ColumnName
import it.carloni.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow, resolveDataType}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.xml.{Elem, XML}

abstract class AbstractInitialOrReloadEngine(val jobPropertiesFile: String)
  extends AbstractEngine(jobPropertiesFile) {

  private final val logger: Logger = Logger.getLogger(getClass)

  protected def readTSVForTable(tableId: String): DataFrame = {

    val tsvFilePath: String = jobProperties.getString(s"table.$tableId.file.path")
    val tsvSep: String = jobProperties.getString(s"table.$tableId.file.sep")
    val tsvHeaderFlag: Boolean = jobProperties.getBoolean(s"table.$tableId.file.header")
    val xMLSchemaFilePath: String = jobProperties.getString(s"table.$tableId.xml.schema.path")

    val details: String = s"path '$tsvFilePath' (separator: '$tsvSep', file header presence: '$tsvHeaderFlag')"
    logger.info(s"Attempting to load .tsv file at $details")

    val tsvFileDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", tsvFilePath)
      .option("sep", tsvSep)
      .option("header", tsvHeaderFlag)
      .schema(fromXMLToStructType(xMLSchemaFilePath))
      .load()
      .withColumn(ColumnName.TS_INIZIO_VALIDITA.name(), lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DT_INIZIO_VALIDITA.getName, lit(getJavaSQLDateFromNow))

    logger.info(s"Successfully loaded .tsv file at $details")
    tsvFileDf
  }

  private def fromXMLToStructType(xmlFilePath: String): StructType = {

    val xmlSchemaFile: File = new File(xmlFilePath)
    if (xmlSchemaFile.exists) {

      logger.info(s"XML file '$xmlFilePath' exists. So, trying to infer table schema from it")
      val mappingSpecificationXML: Elem = XML.loadFile(xmlSchemaFile)
      val columnSpecifications: Seq[(String, String, String)] = (mappingSpecificationXML \\ "tableSchema" \\ "columns" \\ "column")
        .map(columnTag => (columnTag.attribute("name").get.text,
          columnTag.attribute("type").get.text,
          columnTag.attribute("nullable").get.text))

      StructType(columnSpecifications.map(tuple3 => {

        val columnName: String = tuple3._1
        val columnType: DataType = resolveDataType(tuple3._2.toLowerCase)
        val nullable: Boolean = tuple3._3.toLowerCase == "true"

        logger.info(s"Defining column with name '$columnName', type '$columnType', nullable '$nullable'")
        StructField(columnName, columnType, nullable)
      }))

    } else {

      val exceptionMsg: String = s"File '$xmlFilePath' does not exists (or cannot be found)"
      logger.error(exceptionMsg)
      throw new FileNotFoundException(exceptionMsg)
    }
  }
}
