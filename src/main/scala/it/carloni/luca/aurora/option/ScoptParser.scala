package it.carloni.luca.aurora.option

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.aurora.utils.DateFormat
import scopt.OptionParser

import scala.util.Try

object ScoptParser {

  private def objectToString(tClass: Class[_], scoptOptionsMap: Map[ScoptOption.Val, Any]): String = {

    val optionsWithValues: String = (scoptOptionsMap map { case (key, value) =>  s"\t-${key.short}, --${key.long} (${key.text}) = \'$value\'"})
      .toSeq
      .mkString("\n")

    s"${tClass.getSimpleName}: \n"
      .concat(optionsWithValues)
      .concat("\n")
  }

  case class BranchConfig(applicationBranch: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Val, Any] = Map(ScoptOption.applicationBranchOption -> applicationBranch)
      objectToString(getClass, toStringMap)
    }
  }

  case class InitialLoadConfig(propertiesFile: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Val, Any] = Map(ScoptOption.propertiesOption -> propertiesFile)
      objectToString(getClass, toStringMap)
    }
  }

  case class SourceLoadConfig(propertiesFile: String = "",
                              bancllName: String = "",
                              businessDateOpt: Option[String] = None,
                              versionNumberOpt: Option[Double] = None) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Val, Any] = Map(ScoptOption.propertiesOption -> propertiesFile,
        ScoptOption.sourceOption -> bancllName,
        ScoptOption.businessDateOption -> businessDateOpt.orNull,
        ScoptOption.versionNumberOption -> versionNumberOpt.orNull)
      objectToString(getClass, toStringMap)
    }
  }

  case class ReloadConfig(propertiesFile: String = "",
                          mappingSpecificationFlag: Boolean = false,
                          lookUpFlag: Boolean = false,
                          completeOverwriteFlag: Boolean = false) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Val, Any] = Map(ScoptOption.propertiesOption -> propertiesFile,
        ScoptOption.mappingSpecificationFlag -> mappingSpecificationFlag,
        ScoptOption.lookUpSpecificationFlag -> lookUpFlag,
        ScoptOption.completeOverwriteFlag -> completeOverwriteFlag)
      objectToString(getClass, toStringMap)
    }
  }

  private final val scoptProgramName: String = "scopt 3.3.0"

  val branchParser: OptionParser[BranchConfig] = new OptionParser[BranchConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.applicationBranchOption.short, ScoptOption.applicationBranchOption.long)
      .text(ScoptOption.applicationBranchOption.text)
      .required()
      .action((x, c) => c.copy(applicationBranch = x))
  }

  val initialLoadOptionParser: OptionParser[InitialLoadConfig] = new OptionParser[InitialLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.propertiesOption.short, ScoptOption.propertiesOption.long)
      .text(ScoptOption.propertiesOption.text)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))
  }

  val sourceLoadOptionParser: OptionParser[SourceLoadConfig] = new OptionParser[SourceLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.propertiesOption.short, ScoptOption.propertiesOption.long)
      .text(ScoptOption.propertiesOption.text)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[String](ScoptOption.sourceOption.short, ScoptOption.sourceOption.long)
      .text(ScoptOption.sourceOption.text)
      .required()
      .action((x, c) => c.copy(bancllName = x))

    opt[String](ScoptOption.businessDateOption.short, ScoptOption.businessDateOption.long)
      .text(ScoptOption.businessDateOption.text)
      .required()
      .validate(inputDate => {

        val businessDateFormat: String = DateFormat.dtBusinessDate.format
        val tryParseBusinessDate: Try[LocalDate] = Try(LocalDate.parse(inputDate,
          DateTimeFormatter.ofPattern(businessDateFormat)))

        if (tryParseBusinessDate.isSuccess) success
        else failure(s"Cannot parse business date. Provided \'$inputDate\', should follow format \'$businessDateFormat\'")
      })
      .action((x, c) => c.copy(businessDateOpt = Some(x)))

    opt[Double](ScoptOption.versionNumberOption.short, ScoptOption.versionNumberOption.long)
      .text(ScoptOption.versionNumberOption.text)
      .action((x, c) => c.copy(versionNumberOpt = Some(x)))
  }

  val reloadOptionParser: OptionParser[ReloadConfig] = new OptionParser[ReloadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.propertiesOption.short, ScoptOption.propertiesOption.long)
      .text(ScoptOption.propertiesOption.text)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[Unit](ScoptOption.mappingSpecificationFlag.short, ScoptOption.mappingSpecificationFlag.long)
      .text(ScoptOption.mappingSpecificationFlag.text)
      .action((_, c) => c.copy(mappingSpecificationFlag = true))

    opt[Unit](ScoptOption.lookUpSpecificationFlag.short, ScoptOption.lookUpSpecificationFlag.long)
      .text(ScoptOption.lookUpSpecificationFlag.text)
      .action((_, c) => c.copy(lookUpFlag = true))

    opt[Unit](ScoptOption.completeOverwriteFlag.short, ScoptOption.completeOverwriteFlag.long)
      .text(ScoptOption.completeOverwriteFlag.text)
      .action((_, c) => c.copy(completeOverwriteFlag = true))
  }
}
