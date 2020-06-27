package it.carloni.luca.aurora.option

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import it.carloni.luca.aurora.Main.BranchConfig
import it.carloni.luca.aurora.time.DateFormat
import scopt.OptionParser

import scala.util.Try

object ScoptParser {

  case class InitialLoadConfig(propertiesFile: String = "") {

    override def toString: String = s"${getClass.getSimpleName}: " +
      s"\n${ScoptOption.propertiesOption.text}: $propertiesFile"
  }

  case class SourceLoadConfig(propertiesFile: String = "",
                              bancllName: String = "",
                              businessDate: String = "") {

    override def toString: String = {

      s"${getClass.getSimpleName}: " +
        s"\n${ScoptOption.propertiesOption.text}: $propertiesFile, " +
        s"\n${ScoptOption.sourceOption.text}: $bancllName, " +
        s"\n${ScoptOption.businessDateOption.text}: $businessDate"
    }
  }

  case class ReloadConfig(propertiesFile: String = "",
                          mappingSpecificationFlag: Boolean = false,
                          lookUpFlag: Boolean = false,
                          completeOverwriteFlag: Boolean = false) {

    override def toString: String = {

      s"${getClass.getSimpleName}: " +
        s"\n${ScoptOption.propertiesOption.text}: $propertiesFile" +
        s"\n${ScoptOption.mappingSpecificationFlag.text}: $mappingSpecificationFlag" +
        s"\n${ScoptOption.lookUpSpecificationFlag.text}: $lookUpFlag" +
        s"\n${ScoptOption.completeOverwriteFlag.text}: $completeOverwriteFlag"
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

        val businessDateFormat: String = DateFormat.DtBusinessDate.format
        val tryParseBusinessDate: Try[LocalDate] = Try(LocalDate.parse(inputDate,
          DateTimeFormatter.ofPattern(businessDateFormat)))

        if (tryParseBusinessDate.isSuccess) success
        else failure(s"Cannot parse business date. Provided \'$inputDate\', should follow format \'$businessDateFormat\'")
      })
      .action((x, c) => c.copy(businessDate = x))
  }

  val reloadOptionParser: OptionParser[ReloadConfig] = new OptionParser[ReloadConfig](scoptProgramName) {

    opt[String](ScoptOption.propertiesOption.short, ScoptOption.propertiesOption.long)
      .text(ScoptOption.propertiesOption.text)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[Unit](ScoptOption.mappingSpecificationFlag.short, ScoptOption.mappingSpecificationFlag.long)
      .text(ScoptOption.mappingSpecificationFlag.text)
      .action( (_, c) => c.copy(mappingSpecificationFlag = true))

    opt[Unit](ScoptOption.lookUpSpecificationFlag.short, ScoptOption.lookUpSpecificationFlag.long)
      .text(ScoptOption.lookUpSpecificationFlag.text)
      .action( (_, c) => c.copy(lookUpFlag = true))

    opt[Unit](ScoptOption.completeOverwriteFlag.short, ScoptOption.completeOverwriteFlag.long)
      .text(ScoptOption.completeOverwriteFlag.text)
      .action( (_, c) => c.copy(completeOverwriteFlag = true))
  }
}
