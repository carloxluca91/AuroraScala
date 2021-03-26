package it.luca.aurora

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.logging.LazyLogging
import it.luca.aurora.option.BranchConfig
import it.luca.aurora.spark.engine.InitialLoadEngine
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object BranchRunner extends LazyLogging {

  def apply(branchConfig: BranchConfig, args: Seq[String]): Unit = {

    val branchId: String = branchConfig.applicationBranch
    val propertiesFile: String = branchConfig.propertiesFile

    val sparkConf = new SparkConf()
    sparkConf.set("hive.exec.dynamic.partition", "true")
    sparkConf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sparkContext)
    log.info(s"""Initialized both ${classOf[SparkContext].getSimpleName} and ${classOf[HiveContext].getSimpleName}
         |
         |    appName = ${sparkContext.appName},
         |    appId = ${sparkContext.applicationId}
         |""".stripMargin)

    Branch.withId(branchId) match {
      case Branch.InitialLoad => InitialLoadEngine(sqlContext, propertiesFile).run()
      case Branch.Reload =>

        /*
        ScoptParser.reloadOptionParser.parse(args, ReloadConfig())
          .foreach { x =>
            log.info(s"Parsed second set of arguments $x")
            ReLoadEngine(sqlContext, propertiesFile).run(x) }

         */

      case Branch.SourceLoad =>

        /*
        ScoptParser.sourceLoadOptionParser.parse(args, SourceLoadConfig())
          .foreach {x =>
            log.info(s"Parsed second set of arguments $x")
            SourceLoadEngine(sqlContext, propertiesFile).run(x)
          }

         */
      }
  }
}
