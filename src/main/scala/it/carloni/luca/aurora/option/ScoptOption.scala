package it.carloni.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, text: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToScoptOptionVal(x: Value): Val = x.asInstanceOf[Val]

  val applicationBranchOption: Val = Val('b', "branch", "Application branch to run")
  val propertiesOption: Val = Val('p', "properties", "Name of .properties file")
  val sourceOption: Val = Val('s', "source", "Source name (BANCLL) for which ingestion must be triggered")
  val businessDateOption: Val = Val('d', "date", "Working business date")
}
