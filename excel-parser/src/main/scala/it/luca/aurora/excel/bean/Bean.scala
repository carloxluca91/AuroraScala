package it.luca.aurora.excel.bean

import it.luca.aurora.excel.decode.ExcelRowDecoder
import org.apache.poi.ss.usermodel.Row

case class Bean(f1: Option[String],
                f2: String,
                f3: Int,
                f4: Option[String])

object Bean extends ExcelRowDecoder[Bean] {

  override implicit def decode(row: Row): Bean = {

    Bean(f1 = row(0).asOption[String],
      f2 = row(1).as[String],
      f3 = row(2).as[Double].toInt,
      f4 = row(3).asOption[String])
  }
}
