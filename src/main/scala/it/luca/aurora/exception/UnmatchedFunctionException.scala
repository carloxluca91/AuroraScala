package it.luca.aurora.exception

case class UnmatchedFunctionException(functionToApply: String)
  extends Throwable(s"Unable to match such function '$functionToApply'")
