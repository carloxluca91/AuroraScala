package it.carloni.luca.aurora.option

object Branch extends Enumeration {

  val InitialLoad: Branch.Value = Value("INITIAL_LOAD")
  val SourceLoad: Branch.Value = Value("SOURCE_LOAD")
  val ReLoad: Branch.Value = Value("RE_LOAD")
}
