package it.luca.aurora.spark.step


import it.luca.aurora.excel.implicits._
import it.luca.aurora.logging.Logging
import it.luca.aurora.utils.Utils.classSimpleName
import org.apache.poi.ss.usermodel.{Row, Workbook}

import scala.reflect.runtime.universe.TypeTag

case class DecodeSheet[T](override val input: Workbook,
                          override val outputKey: String,
                          private val sheetIndex: Int)(implicit typeTag: TypeTag[T], val decodeRow: Row => T)
  extends IOStep[Workbook, Seq[T]](input, stepName =  s"DECODE_EXCEL_SHEET_$sheetIndex", outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: Workbook): Seq[T] = {

    val tClassName = classSimpleName[T]
    log.info(s"Decoding sheet # $sheetIndex as a Seq of $tClassName")
    val tSeq: Seq[T] = input.as[T](sheetIndex)
    log.info(s"Decoded sheet # $sheetIndex as ${tSeq.size} $tClassName(s)")
    tSeq
  }
}
