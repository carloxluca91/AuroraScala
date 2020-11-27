package it.luca.aurora.spark.engine

import java.io.{File, FileNotFoundException}

import it.luca.aurora.utils.ColumnName
import it.luca.aurora.utils.Utils.{getJavaSQLDateFromNow, getJavaSQLTimestampFromNow, resolveDataType}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.xml.{Elem, XML}

abstract class AbstractInitialOrReloadEngine(override val jobPropertiesFile: String)
  extends AbstractEngine(jobPropertiesFile) {

  private final val logger: Logger = Logger.getLogger(getClass)

  protected final val tableLoadingOptionsMap: Map[String, (String, String, String, String)] = Map(

    mappingSpecificationTBLName -> (jobProperties.getString("table.mappingSpecification.tsv.file"),
      jobProperties.getString("table.mappingSpecification.tsv.sep"),
      jobProperties.getString("table.mapping_specification.file.header"),
      jobProperties.getString("table.mappingSpecification.schema.file")),

    lookupTBLName -> (jobProperties.getString("table.lookup.tsv.file"),
      jobProperties.getString("table.lookup.tsv.sep"),
      jobProperties.getString("table.lookup.file.header"),
      jobProperties.getString("table.lookup.schema.file")))

  protected def readTsvAsDataframe(actualTableName: String): DataFrame = {

    val tsvReadingOptions: (String, String, String, String) = tableLoadingOptionsMap(actualTableName)
    val tsvFilePath: String = tsvReadingOptions._1
    val tsvSep: String = tsvReadingOptions._2
    val tsvHeaderFlag: Boolean = tsvReadingOptions._3.toBoolean
    val xMLSchemaFilePath: String = tsvReadingOptions._4

    val details: String = s"path '$tsvFilePath' (separator: '$tsvSep', file header presence: '$tsvHeaderFlag')"
    logger.info(s"Attempting to load .tsv file at $details")

    val tsvFileDf: DataFrame = sparkSession.read
      .format("csv")
      .option("path", tsvFilePath)
      .option("sep", tsvSep)
      .option("header", tsvHeaderFlag)
      .schema(fromXMLToStructType(xMLSchemaFilePath))
      .load()
      .withColumn(ColumnName.TsInizioValidita.name, lit(getJavaSQLTimestampFromNow))
      .withColumn(ColumnName.DtInizioValidita.name, lit(getJavaSQLDateFromNow))

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
