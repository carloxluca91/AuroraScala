package it.luca.aurora.core

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class CustomSpec
  extends AnyFlatSpec
    with should.Matchers {

  def clazz[T](implicit evidence$1: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

}
