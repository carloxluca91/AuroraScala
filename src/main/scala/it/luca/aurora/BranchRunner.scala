package it.luca.aurora

import it.luca.aurora.enumeration.Branch
import it.luca.aurora.logging.Logging
import it.luca.aurora.option.{BranchConfig, ReloadConfig, ScoptParser, DataSourceLoadConfig}
import it.luca.aurora.spark.job.{InitialLoadJob, ReloadJob, DataSourceLoadJob}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object BranchRunner extends Logging {

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
      case Branch.InitialLoad => InitialLoadJob(sqlContext, propertiesFile).run()
      case Branch.Reload =>
        ScoptParser.reloadOptionParser.parse(args, ReloadConfig()) match {
          case Some(x) =>
            log.info(s"Parsed second set of arguments $x")
            ReloadJob(sqlContext, propertiesFile, x).run()
          case None => // arguments are bad, error message will have been displayed
        }

      case Branch.SourceLoad =>
        ScoptParser.sourceLoadOptionParser.parse(args, DataSourceLoadConfig()) match {
          case Some(x) =>
            log.info(s"Parsed second set of arguments $x")
            DataSourceLoadJob(sqlContext, propertiesFile, x).run()
          case None => // arguments are bad, error message will have been displayed
        }
    }
  }
}
