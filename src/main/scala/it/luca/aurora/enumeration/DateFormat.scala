package it.luca.aurora.enumeration

object DateFormat extends Enumeration {

  protected case class Val(format: String) extends super.Val

  implicit def toVal(x: Value): Val = x.asInstanceOf[Val]

  implicit def asString(x: Value): String = x.format

  val DateDefault: Val = Val("yyyy-MM-dd")

}
