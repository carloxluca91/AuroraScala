package it.luca.aurora

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.option.{BranchConfig, ReloadConfig, ScoptParser, SourceLoadConfig}
import it.luca.aurora.spark.engine.{InitialLoadEngine, ReLoadEngine, SourceLoadEngine}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

object BranchRunner extends Logging {

  def apply(branchConfig: BranchConfig, args: Seq[String]): Unit = {

    val branchId: String = branchConfig.applicationBranch
    val propertiesFile: String = branchConfig.propertiesFile
    Try {

      val sparkContext: SparkContext = new SparkContext(new SparkConf())
      val sqlContext: HiveContext = new HiveContext(sparkContext)
      info(s"""Initialized both ${classOf[SparkContext].getSimpleName} and ${classOf[HiveContext].getSimpleName}
           |
           |    appName = ${sparkContext.appName},
           |    appId = ${sparkContext.applicationId}
           |""".stripMargin)

      Branch.withId(branchId) match {
        case Branch.InitialLoad => InitialLoadEngine(propertiesFile).run()
        case Branch.Reload =>

          ScoptParser.reloadOptionParser.parse(args, ReloadConfig())
            .foreach { x =>
              info(s"Successfully parsed second set of arguments $x")
              ReLoadEngine(propertiesFile).run(x) }

        case Branch.SourceLoad =>

          ScoptParser.sourceLoadOptionParser.parse(args, SourceLoadConfig())
            .foreach {x =>
              info(s"Successfully parsed second set of arguments $x")
              SourceLoadEngine(propertiesFile).run(x)
            }
        }
    } match {
      case Success(_) => info(s"Successfully executed whole branch $branchId")
      case Failure(exception) => error(s"Unable to fully execute branch $branchId. Stack trace: ", exception)
    }
  }
}
