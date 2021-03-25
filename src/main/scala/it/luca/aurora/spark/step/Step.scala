package it.luca.aurora.spark.step

import it.luca.aurora.enumeration.JobVariable

abstract class Step[I](protected val input: I,
                       val name: String)

abstract class IStep[I](override protected val input: I,
                        override val name: String)
  extends Step[I](input, name) {

  def run(): Unit

}

abstract class IOStep[I, O](override protected val input: I,
                            override val name: String)
  extends Step[I](input, name) {

  def run(): (JobVariable.Value, O)
}


