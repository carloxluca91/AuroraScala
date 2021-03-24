package it.luca.aurora.option

import it.luca.aurora.enumeration.ScoptOption

abstract class BaseConfig {

  protected val scoptOptionMap: Map[ScoptOption.Value, String]

  override def toString: String = {

    val givenArguments: String = scoptOptionMap.map { case (key, value) =>
      s"    -${key.shortOption}, --${key.longOption} (${key.optionDescription}) = $value"
    }.mkString(",\n")

    s"""
       |
       |$givenArguments
       |""".stripMargin
  }
}
