package it.carloni.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, text: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToPlanetVal(x: Value): Val = x.asInstanceOf[Val]

  val rawSourceNameOption: Val = Val('r', "raw-source", "Raw source (BANCLL) for which ingestion must be triggered")
  val fileOption: Val = Val('f', "file", "Name of .properties file supplied via --files option of spark-submit")
}
