package it.carloni.luca.aurora.spark.exceptions

class TableNotFoundException(tableName: String)
  extends Exception(TableNotFoundException.msg.format(tableName))

object TableNotFoundException {

  val msg: String = "Unable to find table %s"
}
