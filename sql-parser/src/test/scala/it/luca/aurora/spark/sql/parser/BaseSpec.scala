package it.luca.aurora.spark.sql.parser

import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class BaseSpec extends AnyFlatSpec with should.Matchers {

  protected final val firstColumnName = "first_col"
  protected final val secondColumnName = "second_col"
  protected final val firstColumn = col(firstColumnName)
  protected final val secondColumn = col(secondColumnName)

  def clazz[T](implicit evidence$1: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

}
