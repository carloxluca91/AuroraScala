package it.luca.aurora.spark.step

import it.luca.aurora.excel.implicits._
import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.classSimpleName
import org.apache.poi.ss.usermodel.{Row, Workbook}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

case class DecodeSheet[T](private val workbookKey: String,
                          private val sheetIndex: Int,
                          override val outputKey: String)(implicit typeTag: TypeTag[T], val decodeRow: Row => T)
  extends IOStep[Workbook, Seq[T]](s"DECODE_EXCEL_SHEET_$sheetIndex", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, Seq[T]) = {

    val tClassName = classSimpleName[T]
    val workbook = variables(workbookKey).asInstanceOf[Workbook]
    log.info(s"Decoding sheet # $sheetIndex as a Seq of $tClassName")
    val tSeq: Seq[T] = workbook.as[T](sheetIndex)
    log.info(s"Decoded sheet # $sheetIndex as ${tSeq.size} $tClassName(s)")
    (outputKey, tSeq)
  }
}
