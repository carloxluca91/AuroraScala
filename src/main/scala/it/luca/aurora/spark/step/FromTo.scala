package it.luca.aurora.spark.step

import it.luca.aurora.utils.className

import scala.collection.mutable
import scala.reflect.runtime.universe._

case class FromTo[T, R](private val tInputKey: String,
                        private val tToR: T => R,
                        override val outputKey: String)(implicit tTypeTag: TypeTag[T], rTypeTag: TypeTag[R])
  extends IOStep[T, R](s"Apply a function from ${className[T]} to ${className[R]}", outputKey) {

  override def run(variables: mutable.Map[String, Any]): (String, R) = {

    val t = variables(tInputKey).asInstanceOf[T]
    (outputKey, tToR(t))
  }
}
