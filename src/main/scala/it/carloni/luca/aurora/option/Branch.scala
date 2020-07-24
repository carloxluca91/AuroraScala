package it.carloni.luca.aurora.option

object Branch extends Enumeration {

  protected case class BranchName(name: String) extends super.Val(name)

  import scala.language.implicitConversions

  implicit def asBranchName(x: Value): BranchName = x.asInstanceOf[BranchName]

  val InitialLoad: BranchName = BranchName("INITIAL_LOAD")
  val SourceLoad: BranchName = BranchName("SOURCE_LOAD")
  val ReLoad: BranchName = BranchName("RE_LOAD")
}
