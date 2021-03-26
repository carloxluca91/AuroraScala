package it.luca.aurora

import it.luca.aurora.logging.LazyLogging
import it.luca.aurora.option.{BranchConfig, ScoptParser}

object Main extends App with LazyLogging {

  log.info("Started Aurora - Dataload main class")
  ScoptParser.branchParser.parse(args, BranchConfig())
    .foreach{ x =>
      log.info(s"Parsed application branch and .properties file $x")
      BranchRunner(x, args)
    }
}
