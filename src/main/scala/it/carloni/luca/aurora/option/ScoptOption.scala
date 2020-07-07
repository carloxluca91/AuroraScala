package it.carloni.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class Val(short: Char, long: String, text: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val applicationBranchOption: Val = Val('b', "branch", "Application branch to run")
  val propertiesOption: Val = Val('p', "properties", "Name of .properties file")
  val sourceOption: Val = Val('s', "source", "Source name (BANCLL) for which ingestion must be triggered")
  val businessDateOption: Val = Val('d', "date", "Working business date")
  val mappingSpecificationFlag: Val = Val('m', "mapping_specification", "Flag for overwriting mapping specification table")
  val lookUpSpecificationFlag: Val = Val('l', "look_up", "Flag for overwriting look up table")
  val completeOverwriteFlag: Val = Val('o', "overwrite", "Flag for specifing behavior when reloading tables. " +
    "True: drop and reload. False: truncate and reload")
}
