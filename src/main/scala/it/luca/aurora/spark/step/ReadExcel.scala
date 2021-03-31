package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

import java.io.FileNotFoundException

case class ReadExcel(override val input: String,
                     override val outputKey: String)
  extends IOStep[String, Workbook](input, stepName =  "READ_EXCEL", outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: String): Workbook = {

    val path = new Path(input)
    val fs: FileSystem = FileSystem.get(new Configuration())
    log.info("Opened HDFS connection")
    val workbookClassName = classOf[Workbook].getSimpleName
    if (fs.exists(path) && fs.isFile(path)) {

      val excelFileStatus = fs.getFileStatus(path)
      log.info(s"""HDFS file $input exists. Starting to read it as a $workbookClassName. Some details
           |
           |    owner: ${excelFileStatus.getOwner}
           |    lastModification: ${new java.sql.Timestamp(excelFileStatus.getModificationTime)}
          """.stripMargin)
      val workBook: Workbook = WorkbookFactory.create(fs.open(path))
      log.info(s"Successfully Loaded HDFS file $input as a $workbookClassName")

      fs.close()
      log.info("Closed HDFS connection")
      workBook
    } else {
      fs.close()
      log.info("Closed HDFS connection")
      throw new FileNotFoundException(s"file $input not exists")
    }
  }
}
