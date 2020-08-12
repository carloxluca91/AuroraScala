package it.carloni.luca.aurora.spark.exception

class UnmatchedFunctionException(unmatchedFunctionStr: String)
  extends Exception(UnmatchedFunctionException.msg.format(unmatchedFunctionStr))

object UnmatchedFunctionException {

  private val msg: String = s"Unable to match such function: '%s'"
}
