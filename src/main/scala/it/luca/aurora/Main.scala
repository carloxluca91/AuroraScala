package it.luca.aurora

import grizzled.slf4j.Logging
import it.luca.aurora.option.{BranchConfig, ScoptParser}

object Main extends App with Logging {

  info("Started Aurora - Dataload main class")
  ScoptParser.branchParser.parse(args, BranchConfig())
    .foreach{ x =>
      info(s"Parsed application branch and .properties file $x")
      BranchRunner(x, args)
    }
}
