package it.luca.aurora.spark.step

import it.luca.aurora.utils.classFullName

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

abstract class Step[I](val description: String)(implicit typeTagI: TypeTag[I]) {

  val stepClassName: String = getClass.getName
  val stepInputType: String = classFullName[I]
}

abstract class IStep[I](override val description: String)
                       (implicit typeTagI: TypeTag[I])
  extends Step[I](description) {

  def run(variables: mutable.Map[String, Any]): Unit

}

abstract class IOStep[I, O](override val description: String, val outputKey: String)
                           (implicit typeTagI: TypeTag[I], typeTagO: TypeTag[O])
  extends Step[I](description) {

  val stepOutputType: String = classFullName[O]

  def run(variables: mutable.Map[String, Any]): (String, O)
}


