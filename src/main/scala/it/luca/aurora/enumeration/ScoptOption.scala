package it.luca.aurora.enumeration

object ScoptOption extends Enumeration {

  protected case class Val(shortOption: Char, longOption: String, optionDescription: String)
    extends super.Val

  import scala.language.implicitConversions

  implicit def valueToScoptOptionVal(x: Value): Val = x.asInstanceOf[Val]

  // Required
  val ApplicationBranch: Val = Val('b', "branch", "Application branch to run")
  val PropertiesFile: Val = Val('p', "properties", "Path of .properties file")

  // DataSourceLoad branch additional options
  val DataSource: Val = Val('s', "source", "Datasource to be ingested")
  val DtBusinessDate: Val = Val('d', "date", "Working business date")
  val SpecificationVersion: Val = Val('v', "version", "Specification version number to be referred to")

  // Reload branch additional options
  val MappingSpecificationFlag: Val = Val('m', "mapping-specification", "Flag for overwriting mapping specification table")
  val LookupSpecificationFlag: Val = Val('l', "look-up", "Flag for overwriting look up table")
}
