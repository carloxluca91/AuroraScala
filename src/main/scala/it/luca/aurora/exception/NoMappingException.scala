package it.luca.aurora.exception

import it.luca.aurora.spark.bean.MappingRow
import it.luca.aurora.utils.className

case class NoMappingException(sourceId: String)
  extends Throwable(s"No ${className[MappingRow]} found for $sourceId")
