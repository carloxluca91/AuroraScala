package it.carloni.luca.aurora

import it.carloni.luca.aurora.option.ScoptOption
import it.carloni.luca.aurora.spark.engine.SparkEngine
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

object Main extends App {

  val logger: Logger = Logger.getRootLogger

  logger.info("Starting application main program")

  case class Config(rawSRCName: String = "",
                    applicationPropertiesFile: String = "") {

    override def toString: String =

      s"${ScoptOption.rawSourceNameOption.text} = $rawSRCName, " +
        s"${ScoptOption.fileOption.text} = $applicationPropertiesFile"
  }

  val optionParser: OptionParser[Config] = new OptionParser[Config]("scopt 3.3.0") {

    opt[String](ScoptOption.rawSourceNameOption.shortOption, ScoptOption.rawSourceNameOption.longOption)
      .text(ScoptOption.rawSourceNameOption.text)
      .required()
      .action((x, c) => c.copy(rawSRCName = x))

    opt[String](ScoptOption.fileOption.shortOption, ScoptOption.fileOption.longOption)
      .text(ScoptOption.fileOption.text)
      .required()
      .action((x, c) => c.copy(applicationPropertiesFile = x))

  }

  optionParser.parse(args, Config()) match {

    case Some(value) =>

      logger.info("Successfully parsed command line args")
      logger.info(value.toString)

      val applicationName: String = s"Aurora - DataLoad(${value.rawSRCName})"
      val sparkContext: SparkContext = new SparkContext(new SparkConf().setAppName(applicationName))
      new SparkEngine(sparkContext, value.applicationPropertiesFile).run(value.rawSRCName)

    case None => logger.error("Error during parsing of command line args")
  }
}
