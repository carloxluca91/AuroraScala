package it.luca.aurora.spark.step

import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.classSimpleName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

import java.io.FileNotFoundException
import scala.collection.mutable

case class ReadExcel(private val excelPath: String,
                     override val outputKey: String)
  extends IOStep[String, Workbook]("Read Excel (.xlsx) file", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, Workbook) = {

    val path = new Path(excelPath)
    val fs: FileSystem = FileSystem.get(new Configuration())
    log.info("Opened HDFS connection")
    val workbookClassName = classSimpleName[Workbook]
    if (fs.exists(path) && fs.isFile(path)) {

      val excelFileStatus = fs.getFileStatus(path)
      log.info(s"""HDFS file $excelPath exists. Starting to read it as a $workbookClassName. Some details
                  |
                  |    File owner: ${excelFileStatus.getOwner}
                  |    File owner group: ${excelFileStatus.getGroup}
                  |    File size (in KB): ${excelFileStatus.getLen / 1000}
                  |    Last modification time: ${new java.sql.Timestamp(excelFileStatus.getModificationTime)}
          """.stripMargin)
      val workBook: Workbook = WorkbookFactory.create(fs.open(path))
      log.info(s"Loaded HDFS file $excelPath as a $workbookClassName")
      (outputKey, workBook)
    } else {
      throw new FileNotFoundException(s"HDFS file $excelPath not exists")
    }
  }
}
