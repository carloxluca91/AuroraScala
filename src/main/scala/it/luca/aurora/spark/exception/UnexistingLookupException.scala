package it.luca.aurora.spark.exception

import it.luca.aurora.utils.ColumnName

case class UnexistingLookupException(exceptionMsg: String)
extends Exception(exceptionMsg)

object UnexistingLookupException {

  def apply(colonnaTd: String, tipoLookup: String, lookupId: String): UnexistingLookupException = {

    val msg = s"Unable to retrieve any lookup case related to trusted column '$colonnaTd' " +
      s"(${ColumnName.LookupTipo.name.toUpperCase} = '$tipoLookup', " +
      s"${ColumnName.LookupId.name.toUpperCase} = '$lookupId')"

    UnexistingLookupException(msg)
  }
}
