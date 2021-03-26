package it.luca.aurora.spark.step

abstract class Step[I](protected val input: I,
                       val stepName: String) {

  val stepInputType: String = classOf[I].getName
}

abstract class IStep[I](override protected val input: I,
                        override val stepName: String)
  extends Step[I](input, stepName) {

  def run(): Unit

}

abstract class IOStep[I, O](override protected val input: I,
                            override val stepName: String,
                            protected val outputKey: String)
  extends Step[I](input, stepName) {

  val stepOutputType: String = classOf[O].getName

  protected def stepFunction(input: I): O

  def run(): (String, O) = (outputKey, stepFunction(input))
}


