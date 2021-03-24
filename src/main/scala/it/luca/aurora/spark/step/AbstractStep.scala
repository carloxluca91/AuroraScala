package it.luca.aurora.spark.step

abstract class AbstractStep[I, O](val name: String) {

  def run(input: I): O
}
