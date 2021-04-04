package it.luca.aurora.option

import it.luca.aurora.enumeration.ScoptOption

case class ReloadConfig(specificationFlag: Boolean = false, lookUpFlag: Boolean = false)
  extends BaseConfig {

  protected val scoptOptionMap: Map[ScoptOption.Value, String] = Map(ScoptOption.MappingSpecificationFlag -> specificationFlag.toString,
    ScoptOption.LookupSpecificationFlag -> lookUpFlag.toString)
}
