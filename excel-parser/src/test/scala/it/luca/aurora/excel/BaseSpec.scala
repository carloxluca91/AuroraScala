package it.luca.aurora.excel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class BaseSpec
  extends AnyFlatSpec
    with should.Matchers {

  def clazz[T](implicit evidence$1: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

}
