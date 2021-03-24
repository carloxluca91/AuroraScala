package it.luca.aurora

import grizzled.slf4j.Logging
import it.luca.aurora.enumeration.Branch
import it.luca.aurora.option.ScoptParser.{InitialLoadConfig, ReloadConfig, SourceLoadConfig}
import it.luca.aurora.option.{BranchConfig, ScoptParser}
import it.luca.aurora.spark.engine.{InitialLoadEngine, ReLoadEngine, SourceLoadEngine}

object Main extends App with Logging {

  info("Started Aurora - Dataload main class")

  // First, parse arguments in order to detect branch to be run
  ScoptParser.branchParser.parse(args, BranchConfig()) match {

    case Some(branchConfig) =>

      info(s"Successfully parsed first set of arguments (application branch and .properties file) $branchConfig")
      Branch.withId(branchConfig.applicationBranch) match {

        case Branch.InitialLoad => InitialLoadEngine(branchConfig.propertiesFile).run()
        case Branch.SourceLoad =>

          ScoptParser.sourceLoadOptionParser.parse(args, SourceLoadConfig()) match {

            case None => error("Error on second set of arguments")
            case Some(value) =>

              info(s"Successfully parsed second set of arguments $value")
              SourceLoadEngine(branchConfig.propertiesFile).run(value)
          }

        case Branch.Reload =>

          ScoptParser.reloadOptionParser.parse(args, ReloadConfig()) match {

            case None => error("Error on second set of arguments")
            case Some(value) =>

              info(s"Successfully parsed second set of arguments $value")
              ReLoadEngine(value.propertiesFile).run(value)
          }
      }

    case None => error("Error during parsing of first set of arguments (application branch)")
  }
}
