package it.luca.aurora.spark.engine

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.spark.step.{AbstractStep, CreateDbStep}
import org.apache.spark.sql.SQLContext

case class InitialLoadEngine(override protected val sqlContext: SQLContext, 
                             override protected val propertiesFile: String)
  extends AbstractEngine(sqlContext, propertiesFile) 
    with Logging {

  override protected val branch: Branch.Value = Branch.InitialLoad
  override protected val dataSource: Option[String] = None
  override protected val dtBusinessDate: Option[String] = None
  override protected val specificationVersion: Option[String] = None

  override protected val steps: Seq[(AbstractStep[_, _], _)] = (CreateDbStep(sqlContext), dbName) :: Nil
}

