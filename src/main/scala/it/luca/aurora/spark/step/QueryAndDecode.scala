package it.luca.aurora.spark.step

import it.luca.aurora.Logging
import it.luca.aurora.utils.className
import it.luca.aurora.spark.implicits._
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.reflect.runtime.universe._

case class QueryAndDecode[T <: Product](private val query: String,
                                        private val sqlContext: SQLContext,
                                        override val outputKey: String)(implicit typeTag: TypeTag[T])
  extends IOStep[String, Seq[T]](s"Execute HiveQL query and decode resulset to a Seq[${className[T]}]", outputKey)
    with Logging {

  override def run(variables: mutable.Map[String, Any]): (String, Seq[T]) = {

    import sqlContext.implicits._

    val beans: Seq[T] = sqlContext.sql(query)
      .withJavaNamingConvention()
      .as[T].collect().toSeq

    log.info(s"Retrieved ${beans.length} ${className[T]} using query $query")
    (outputKey, beans)
  }
}
