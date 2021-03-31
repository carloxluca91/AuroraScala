package it.luca.aurora.spark.step

import it.luca.aurora.utils.Utils.classFullName

import scala.reflect.runtime.universe.TypeTag

abstract class Step[I](val input: I,
                       val stepName: String)(implicit typeTagI: TypeTag[I]) {

  val stepInputType: String = classFullName[I]
}

abstract class IStep[I](override val input: I,
                        override val stepName: String)(implicit typeTagI: TypeTag[I])
  extends Step[I](input, stepName) {

  def run(): Unit

}

abstract class IOStep[I, O](override val input: I,
                            override val stepName: String,
                            protected val outputKey: String) (implicit typeTagI: TypeTag[I], typeTagO: TypeTag[O])
  extends Step[I](input, stepName) {

  val stepOutputType: String = classFullName[O]

  protected def stepFunction(input: I): O

  def run(): (String, O) = (outputKey, stepFunction(input))
}


