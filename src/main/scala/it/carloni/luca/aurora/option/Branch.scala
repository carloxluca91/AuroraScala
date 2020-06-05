package it.carloni.luca.aurora.option

object Branch extends Enumeration {

  type Branch = Value

  val initialLoad: Value = Value("INITIAL_LOAD")
  val sourceLoad: Value = Value("SOURCE_LOAD")
}
