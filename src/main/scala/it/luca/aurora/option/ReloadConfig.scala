package it.luca.aurora.option

import it.luca.aurora.enumeration.ScoptOption

case class ReloadConfig(mappingSpecificationFlag: Boolean = false,
                        lookUpFlag: Boolean = false,
                        completeOverwriteFlag: Boolean = false)
  extends BaseConfig {

  protected val scoptOptionMap: Map[ScoptOption.Value, String] = Map(ScoptOption.MappingSpecificationFlag -> mappingSpecificationFlag.toString,
    ScoptOption.LookupSpecificationFlag -> lookUpFlag.toString,
    ScoptOption.CompleteOverwriteFlag -> completeOverwriteFlag.toString)
}
