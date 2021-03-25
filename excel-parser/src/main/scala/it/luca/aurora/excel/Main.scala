package it.luca.aurora.excel

import grizzled.slf4j.Logging
import it.luca.aurora.excel.bean.Bean
import it.luca.aurora.excel.implicits._

object Main extends App with Logging {

  info("Vai")

  val path = "C:\\Users\\ThinkOpen\\IdeaProjects\\privati\\aurora-dataload\\excel-parser\\src\\main\\resources\\test.xlsx"

  val s: Seq[Bean] = ExcelReader.read(path).as[Bean](0, skipHeader = true)

  info(s"Seq size: ${s.size}. ${s.map(_.toString).mkString(", ")}")
}
