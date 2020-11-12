package it.luca.aurora.utils

import java.time.format.DateTimeFormatter

object DateFormat extends Enumeration {

  protected case class Val(format: String, formatter: DateTimeFormatter)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToDateFormatVal(x: Value): Val = x.asInstanceOf[Val]

  val DtRiferimento: Val = Val("yyyy-MM-dd", DateTimeFormatter.ISO_LOCAL_DATE)

}
