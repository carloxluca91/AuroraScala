package it.carloni.luca.aurora

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.aurora.option.{Branch, ScoptOption}
import it.carloni.luca.aurora.spark.engine.{InitialLoadEngine, SourceLoadEngine}
import it.carloni.luca.aurora.time.DateFormat
import org.apache.log4j.Logger
import scopt.OptionParser

import scala.util.Try

object Main extends App {

  val logger: Logger = Logger.getRootLogger

  logger.info("Starting application main program")

  case class BranchConfig(applicationBranch: String = "") {

    override def toString: String = s"${ScoptOption.applicationBranchOption.text} = $applicationBranch"
  }

  // FIRST, PARSE ARGUMENTS IN ORDER TO DETECT BRANCH TO BE RUN
  val branchParser: OptionParser[BranchConfig] = new OptionParser[BranchConfig]("scopt 3.3.0") {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false
    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.applicationBranchOption.shortOption, ScoptOption.applicationBranchOption.longOption)
      .text(ScoptOption.applicationBranchOption.text)
      .required()
      .action((x, c) => c.copy(applicationBranch = x))
  }

  branchParser.parse(args, BranchConfig()) match {

    case None => logger.error("Error during parsing of first set of arguments (application branch)")
    case Some(value) =>

      logger.info("Successfully parsed first set of arguments (application branch)")
      logger.info(value.toString)

      // DETECT BRANCH TO BE RUN
      Branch.asBranchName(Branch.withName(value.applicationBranch)) match {

        case Branch.InitialLoad =>

          logger.info(s"Matched branch \'${Branch.InitialLoad.name}\'")
          case class InitialLoadConfig(propertiesFile: String = "") {

            override def toString: String = s"${ScoptOption.propertiesOption.text}: $propertiesFile"
          }

          val initialLoadOptionParser: OptionParser[InitialLoadConfig] = new OptionParser[InitialLoadConfig]("scopt 3.3.0") {

            // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
            override def errorOnUnknownArgument = false
            override def reportWarning(msg: String): Unit = {}

            opt[String](ScoptOption.propertiesOption.shortOption, ScoptOption.propertiesOption.longOption)
              .text(ScoptOption.propertiesOption.text)
              .required()
              .action((x, c) => c.copy(propertiesFile = x))
          }

          initialLoadOptionParser.parse(args, InitialLoadConfig()) match {

            case None => logger.error("Error during parsing of second set of arguments (branch arguments)")
            case Some(value) =>

              logger.info(value)
              logger.info("Successfully parsed second set of arguments (branch arguments)")
              new InitialLoadEngine(value.propertiesFile).run()
          }

        case Branch.SourceLoad =>

          logger.info(s"Matched branch \'${Branch.SourceLoad.name}\'")

          case class SourceLoadConfig(propertiesFile: String = "",
                                      bancllName: String = "",
                                      businessDate: String = "") {

            override def toString: String = {

              s"${ScoptOption.propertiesOption.text}: $propertiesFile, " +
                s"${ScoptOption.sourceOption.text}: $bancllName, " +
                s"${ScoptOption.businessDateOption.text}: $businessDate"
            }
          }

          val sourceLoadOptionParser: OptionParser[SourceLoadConfig] = new OptionParser[SourceLoadConfig]("scopt 3.3.0") {

            // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
            override def errorOnUnknownArgument = false
            override def reportWarning(msg: String): Unit = {}

            opt[String](ScoptOption.propertiesOption.shortOption, ScoptOption.propertiesOption.longOption)
              .text(ScoptOption.propertiesOption.text)
              .required()
              .action((x, c) => c.copy(propertiesFile = x))

            opt[String](ScoptOption.sourceOption.shortOption, ScoptOption.sourceOption.longOption)
              .text(ScoptOption.sourceOption.text)
              .required()
              .action((x, c) => c.copy(bancllName = x))

            opt[String](ScoptOption.businessDateOption.shortOption, ScoptOption.businessDateOption.longOption)
              .text(ScoptOption.businessDateOption.text)
              .required()
              .validate(inputDate => {

                val businessDateFormat: String = DateFormat.DtBusinessDate.format
                val tryParseBusinessDate: Try[LocalDate] = Try(LocalDate.parse(inputDate,
                  DateTimeFormatter.ofPattern(businessDateFormat)))

                if (tryParseBusinessDate.isSuccess) success
                else failure(s"Cannot parse business date. Provided \'$inputDate\', should follow format \'$businessDateFormat\'")
              })
              .action((x, c) => c.copy(businessDate = x))
          }

          sourceLoadOptionParser.parse(args, SourceLoadConfig()) match {

            case None => logger.error("Error during parsing of second set of arguments (branch arguments)")
            case Some(value) =>

              logger.info(value)
              logger.info("Successfully parsed second set of arguments (branch arguments)")
              new SourceLoadEngine(value.propertiesFile).run(value.bancllName)
          }
      }
  }
}
