package it.luca.aurora.option

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, optionDescription: String)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToScoptOptionVal(x: Value): Val = x.asInstanceOf[Val]

  val ApplicationBranch: Val = Val('b', "branch", "Application branch to run")
  val PropertiesFile: Val = Val('p', "properties", "Path of .properties file")
  val Source: Val = Val('s', "source", "Source name (BANCLL) for which ingestion must be triggered")
  val DtRiferimento: Val = Val('d', "date", "Working business date")
  val MappingSpecificationFlag: Val = Val('m', "mapping_specification", "Flag for overwriting mapping specification table")
  val LookupSpecificationFlag: Val = Val('l', "look_up", "Flag for overwriting look up table")
  val CompleteOverwriteFlag: Val = Val('o', "overwrite", "Flag for specifing behavior when reloading tables. " +
    "True: drop and reload. False: truncate and reload")
  val VersionNumber: Val = Val('v', "version", "Specification version number to be referred to")

}
