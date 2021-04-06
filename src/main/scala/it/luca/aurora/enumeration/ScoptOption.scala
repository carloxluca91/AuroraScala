package it.luca.aurora.enumeration

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, optionDescription: String)
    extends super.Val

  implicit def valueToScoptOptionVal(x: Value): Val = x.asInstanceOf[Val]

  // Required
  val ApplicationBranch: Val = Val('b', "branch", "Application branch to run")
  val PropertiesFile: Val = Val('p', "properties", "Path of .properties file")

  // DataSourceLoad branch additional options
  val DataSource: Val = Val('s', "source", "Datasource to be ingested")
  val DtBusinessDate: Val = Val('d', "date", "Working business date")
  val SpecificationVersion: Val = Val('v', "version", "Specification version to be referred to")
}
