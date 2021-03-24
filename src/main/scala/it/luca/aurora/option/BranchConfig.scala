package it.luca.aurora.option

import it.luca.aurora.enumeration.ScoptOption

case class BranchConfig(applicationBranch: String = "N.P.",
                        propertiesFile: String = "N.P.")
  extends BaseConfig {

  protected val scoptOptionMap: Map[ScoptOption.Value, String] = Map(
    ScoptOption.ApplicationBranch -> applicationBranch,
    ScoptOption.PropertiesFile -> propertiesFile)
}
