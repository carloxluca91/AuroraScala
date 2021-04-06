package it.luca.aurora.enumeration

object Branch extends Enumeration {

  protected case class Val(name: String)
    extends super.Val

  implicit def valueToBranchVal(x: Value): Val = x.asInstanceOf[Val]

  val InitialLoad: Val = Val("INITIAL_LOAD")
  val Reload: Val = Val("RELOAD")
  val DataSourceLoad: Val = Val("DATASOURCE_LOAD")

  def exists(id: String): Boolean = Branch.values.exists(_.name.equalsIgnoreCase(id))

  def withId(id: String): Branch.Value = Branch.values
    .filter(_.name.equalsIgnoreCase(id)).head
}
