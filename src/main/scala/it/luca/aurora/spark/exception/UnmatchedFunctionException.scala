package it.luca.aurora.spark.exception

case class UnmatchedFunctionException(functionToApply: String)
  extends Exception(s"Unable to match such function '$functionToApply'")
