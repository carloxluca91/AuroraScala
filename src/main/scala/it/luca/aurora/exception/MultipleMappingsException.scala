package it.luca.aurora.exception

import it.luca.aurora.spark.bean.MappingRow
import it.luca.aurora.utils.className

case class MultipleMappingsException(exceptionMsg: String)
  extends Throwable(exceptionMsg)

object MultipleMappingsException {

  def apply(mappingRows: Seq[MappingRow]): MultipleMappingsException = {

    val sourceId: String = mappingRows.map(_.dataSource).distinct.head
    val msg = s"Found ${mappingRows.length} ${className[MappingRow]}(s) for dataSource '$sourceId')"
    MultipleMappingsException(msg)
  }
}