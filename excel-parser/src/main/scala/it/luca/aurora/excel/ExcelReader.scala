package it.luca.aurora.excel

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

import java.io.FileNotFoundException

object ExcelReader extends Logging {

  def read(excelPath: String): Workbook = {

    val path = new Path(excelPath)
    val fs: FileSystem = FileSystem.get(new Configuration())
    info("Opened HDFS connection")
    val workbookClassName = classOf[Workbook].getSimpleName
    if (fs.exists(path) && fs.isFile(path)) {

      info(s"HDFS file $excelPath exists. Starting to read it as a $workbookClassName")
      val workBook: Workbook = WorkbookFactory.create(fs.open(path))
      info(s"Successfully Loaded HDFS file $excelPath as a $workbookClassName")

      fs.close()
      info("Closed HDFS connection")
      workBook
    } else {
      fs.close()
      info("Closed HDFS connection")
      throw new FileNotFoundException(s"HDFS file $excelPath not exists")
    }
  }
}
