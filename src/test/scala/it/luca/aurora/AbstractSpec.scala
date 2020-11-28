package it.luca.aurora

import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class AbstractSpec extends AnyFlatSpec with should.Matchers {

  protected final val jobProperties = new PropertiesConfiguration
  jobProperties.load(getClass.getClassLoader.getResourceAsStream("spark_job.properties"))

  final def className[T](implicit typetag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

}
