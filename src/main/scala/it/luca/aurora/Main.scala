package it.luca.aurora

import it.luca.aurora.option.ScoptParser.{BranchConfig, InitialLoadConfig, ReloadConfig, SourceLoadConfig}
import it.luca.aurora.option.{Branch, ScoptParser}
import it.luca.aurora.spark.engine.{InitialLoadEngine, ReLoadEngine, SourceLoadEngine}
import org.apache.log4j.Logger

object Main extends App {

  val logger: Logger = Logger.getRootLogger

  logger.info("Starting application main program")

  // First, parse arguments in order to detect branch to be run
  ScoptParser.branchParser.parse(args, BranchConfig()) match {

    case None => logger.error("Error during parsing of first set of arguments (application branch)")
    case Some(value) =>

      logger.info("Successfully parsed first set of arguments (application branch)")
      logger.info(value)

      val branchesSet: Set[Branch.Value] = Branch.values
        .filter(_.name.equalsIgnoreCase(value.applicationBranch))

      if (branchesSet.nonEmpty) {

        // Which branch ?
        branchesSet.head match {

          // [a] InitialLoad
          case Branch.InitialLoad =>

            logger.info(s"Matched branch '${Branch.InitialLoad.name}'")
            ScoptParser.initialLoadOptionParser.parse(args, InitialLoadConfig()) match {

              case None => logger.error("Error during parsing of second set of arguments (branch arguments)")
              case Some(value) =>

                logger.info(value)
                logger.info("Successfully parsed second set of arguments (branch arguments)")
                InitialLoadEngine(value.propertiesFile).run()
                logger.info(s"Successfully executed operations on branch '${Branch.InitialLoad.name}'")
            }

          // [b] SourceLoad
          case Branch.SourceLoad =>

            logger.info(s"Matched branch '${Branch.SourceLoad.name}'")
            ScoptParser.sourceLoadOptionParser.parse(args, SourceLoadConfig()) match {

              case None => logger.error("Error during parsing of second set of arguments (branch arguments)")
              case Some(value) =>

                logger.info(value)
                logger.info("Successfully parsed second set of arguments (branch arguments)")
                SourceLoadEngine(value.propertiesFile).run(value)
                logger.info(s"Successfully executed operations on branch '${Branch.SourceLoad.name}'")
            }

          // [c] ReLoad
          case Branch.Reload =>

            logger.info(s"Matched branch '${Branch.Reload.name}'")
            ScoptParser.reloadOptionParser.parse(args, ReloadConfig()) match {

              case None => logger.error("Error during parsing of second set of arguments (branch arguments)")
              case Some(value) =>

                logger.info(value)
                logger.info("Successfully parsed second set of arguments (branch arguments)")
                ReLoadEngine(value.propertiesFile).run(value)
                logger.info(s"Successfully executed operations on branch '${Branch.Reload.name}'")
            }
        }

      } else logger.error(s"Unable to match provided branch: '${value.applicationBranch}'")
  }
}
