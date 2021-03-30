package it.luca.aurora.spark.step


import it.luca.aurora.excel.implicits._
import it.luca.aurora.logging.Logging
import org.apache.poi.ss.usermodel.{Row, Workbook}

case class DecodeSheet[T](override protected val input: Workbook,
                          override protected val outputKey: String,
                          private val sheetIndex: Int)(private implicit val decodeRow: Row => T)
  extends IOStep[Workbook, Seq[T]](input, stepName =  s"DECODE_EXCEL_SHEET_$sheetIndex", outputKey = outputKey)
    with Logging {

  override protected def stepFunction(input: Workbook): Seq[T] = {

    log.info(s"Decoding sheet # $sheetIndex as a ${classOf[Seq[T]].getSimpleName}")
    val tSeq: Seq[T] = input.as[T](sheetIndex)
    log.info(s"Decoded sheet # $sheetIndex as ${tSeq.size} ${classOf[T].getSimpleName}(s)")
    tSeq
  }
}
