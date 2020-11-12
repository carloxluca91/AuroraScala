package it.luca.aurora.option

object Branch extends Enumeration {

  protected case class Val(name: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToBranchVal(x: Value): Val = x.asInstanceOf[Val]

  val InitialLoad: Val = Val("INITIAL_LOAD")
  val Reload: Val = Val("RELOAD")
  val SourceLoad: Val = Val("SOURCE_LOAD")

}
