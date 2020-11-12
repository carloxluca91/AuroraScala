package it.luca.aurora.utils

object TableId extends Enumeration {

  protected case class Val(tableId: String)
    extends super.Val

  import scala.language.implicitConversions
  implicit def valueToTableIdVal(x: Value): Val = x.asInstanceOf[Val]

  val MappingSpecification: Val = Val("mapping_specification")
  val Lookup: Val = Val("lookup")

}
