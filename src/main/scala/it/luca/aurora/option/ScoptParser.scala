package it.luca.aurora.option

import java.time.LocalDate

import it.luca.aurora.utils.DateFormat
import scopt.OptionParser

import scala.util.Try

object ScoptParser {

  private def objectToString(tClass: Class[_], scoptOptionsMap: Map[ScoptOption.Value, Any]): String = {

    val optionsWithValues: String = (scoptOptionsMap map { case (key, value) =>  s"\t-${key.shortOption}, " +
      s"--${key.longOption} (${key.optionDescription}) = '$value'"})
      .toSeq
      .mkString("\n")

    s"${tClass.getSimpleName}: \n"
      .concat(optionsWithValues)
      .concat("\n")
  }

  case class BranchConfig(applicationBranch: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.ApplicationBranch -> applicationBranch)
      objectToString(getClass, toStringMap)
    }
  }

  case class InitialLoadConfig(propertiesFile: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile)
      objectToString(getClass, toStringMap)
    }
  }

  case class SourceLoadConfig(propertiesFile: String = "",
                              bancllName: String = "",
                              dtRiferimentoOpt: Option[String] = None,
                              versionNumberOpt: Option[String] = None) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile,
        ScoptOption.Source -> bancllName,
        ScoptOption.DtRiferimento -> dtRiferimentoOpt.orNull,
        ScoptOption.VersionNumber -> versionNumberOpt.orNull)
      objectToString(getClass, toStringMap)
    }
  }

  case class ReloadConfig(propertiesFile: String = "",
                          mappingSpecificationFlag: Boolean = false,
                          lookUpFlag: Boolean = false,
                          completeOverwriteFlag: Boolean = false) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile,
        ScoptOption.MappingSpecificationFlag -> mappingSpecificationFlag,
        ScoptOption.LookupSpecificationFlag -> lookUpFlag,
        ScoptOption.CompleteOverwriteFlag -> completeOverwriteFlag)
      objectToString(getClass, toStringMap)
    }
  }

  private final val scoptProgramName: String = "scopt 3.3.0"

  val branchParser: OptionParser[BranchConfig] = new OptionParser[BranchConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.ApplicationBranch.shortOption, ScoptOption.ApplicationBranch.longOption)
      .text(ScoptOption.ApplicationBranch.optionDescription)
      .required()
      .action((x, c) => c.copy(applicationBranch = x))
  }

  val initialLoadOptionParser: OptionParser[InitialLoadConfig] = new OptionParser[InitialLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.optionDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))
  }

  val sourceLoadOptionParser: OptionParser[SourceLoadConfig] = new OptionParser[SourceLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.optionDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[String](ScoptOption.Source.shortOption, ScoptOption.Source.longOption)
      .text(ScoptOption.Source.optionDescription)
      .required()
      .action((x, c) => c.copy(bancllName = x))

    opt[String](ScoptOption.DtRiferimento.shortOption, ScoptOption.DtRiferimento.longOption)
      .text(ScoptOption.DtRiferimento.optionDescription)
      .validate(inputDate => {

        val tryParseDtRiferimento: Try[LocalDate] = Try(LocalDate.parse(inputDate, DateFormat.DtRiferimento.formatter))
        if (tryParseDtRiferimento.isSuccess) success
        else failure(s"Cannot parse business date. Provided '$inputDate', should follow format '${DateFormat.DtRiferimento.format}'")
      })
      .action((x, c) => c.copy(dtRiferimentoOpt = Some(x)))

    opt[String](ScoptOption.VersionNumber.shortOption, ScoptOption.VersionNumber.longOption)
      .text(ScoptOption.VersionNumber.optionDescription)
      .action((x, c) => c.copy(versionNumberOpt = Some(x)))
  }

  val reloadOptionParser: OptionParser[ReloadConfig] = new OptionParser[ReloadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.optionDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[Unit](ScoptOption.MappingSpecificationFlag.shortOption, ScoptOption.MappingSpecificationFlag.longOption)
      .text(ScoptOption.MappingSpecificationFlag.optionDescription)
      .action((_, c) => c.copy(mappingSpecificationFlag = true))

    opt[Unit](ScoptOption.LookupSpecificationFlag.shortOption, ScoptOption.LookupSpecificationFlag.longOption)
      .text(ScoptOption.LookupSpecificationFlag.optionDescription)
      .action((_, c) => c.copy(lookUpFlag = true))

    opt[Unit](ScoptOption.CompleteOverwriteFlag.shortOption, ScoptOption.CompleteOverwriteFlag.longOption)
      .text(ScoptOption.CompleteOverwriteFlag.optionDescription)
      .action((_, c) => c.copy(completeOverwriteFlag = true))
  }
}
