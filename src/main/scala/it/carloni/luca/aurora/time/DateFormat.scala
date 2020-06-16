package it.carloni.luca.aurora.time

object DateFormat extends Enumeration {

  protected case class DateFormat(format: String) extends super.Val

  import scala.language.implicitConversions
  implicit def asDateFormat(x: Value): DateFormat = x.asInstanceOf[DateFormat]

  val DtBusinessDate: DateFormat = DateFormat("yyyy-MM-dd")

}
