package it.luca.aurora.option

import it.luca.aurora.enumeration.ScoptOption

case class DataSourceLoadConfig(dataSource: String = "N.P.",
                                dtBusinessDate: Option[String] = None,
                                specificationVersion: Option[String] = None)
  extends BaseConfig {

  protected val scoptOptionMap: Map[ScoptOption.Value, String] = Map(ScoptOption.DataSource -> dataSource,
    ScoptOption.DtBusinessDate -> dtBusinessDate.getOrElse("LATEST"),
    ScoptOption.SpecificationVersion -> specificationVersion.getOrElse("LATEST"))
}