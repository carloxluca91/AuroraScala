package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.{Branch, JobVariable}
import it.luca.aurora.excel.bean.SpecificationRow
import it.luca.aurora.spark.step._
import org.apache.poi.ss.usermodel.Workbook
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class InitialLoadEngine(override protected val sqlContext: SQLContext, 
                             override protected val propertiesFile: String)
  extends AbstractEngine(sqlContext, propertiesFile, Branch.InitialLoad)
    with Logging {

  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  private val excelPath = jobProperties.getString("hdfs.excel.path")
  private val specificationTableName = jobProperties.getString("hive.table.specification.name")

  override protected val steps: Seq[Step[_]] = CreateDbStep(dbName, sqlContext) ::
    ReadExcel(excelPath) ::
    DecodeExcelSheet[SpecificationRow](as[Workbook](JobVariable.ExcelWorkbook), 0, skipHeader = true) ::
    ToDataFrame[SpecificationRow](as[Seq[SpecificationRow]](JobVariable.ExcelDecodedBeans), sqlContext, JobVariable.SpecificationDf) ::
    WriteDataFrame(as[DataFrame](JobVariable.SpecificationDf), dbName, specificationTableName, SaveMode.ErrorIfExists, None) :: Nil
}

