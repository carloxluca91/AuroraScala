package it.luca.aurora.excel

import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Row, Workbook, WorkbookFactory}

import java.io.FileNotFoundException
import scala.collection.JavaConverters._

object ExcelReader extends Logging {

  def apply[T](excelPath: String, sheetIndex: Int, skipHeader: Boolean, rowToT: Row => T): Seq[T] = {

    val path = new Path(excelPath)
    val fs: FileSystem = FileSystem.get(new Configuration())
    val workbookClassName = classOf[Workbook].getSimpleName
    if (fs.exists(path) && fs.isFile(path)) {

      info(s"HDFS file $excelPath exists. Starting to read it as a $workbookClassName")
      val workBook: Workbook = WorkbookFactory.create(fs.open(path))
      info(s"Successfully Loaded HDFS file $excelPath as a $workbookClassName")
      val seqOfTs: Seq[T] = workBook.getSheetAt(sheetIndex)
        .rowIterator().asScala.toSeq
        .map(rowToT)

      info(s"Successfully turned HDFS file $excelPath to ${seqOfTs.size} ${classOf[T].getSimpleName}(s)")
      seqOfTs

    } else {
      throw new FileNotFoundException(s"HDFS file $excelPath not exists")
    }
  }
}
