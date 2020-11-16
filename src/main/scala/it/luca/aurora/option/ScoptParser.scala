package it.luca.aurora.option

import java.time.LocalDate

import it.luca.aurora.utils.DateFormat
import scopt.OptionParser

import scala.util.Try

object ScoptParser {

  abstract class BaseConfig {

    val scoptOptionMap: Map[ScoptOption.Value, Any]

    override def toString: String = {

      val optionsWithValues: String = (scoptOptionMap map { case (key, value) => s"\t-${key.shortOption}, " +
        s"--${key.longOption} (${key.optionDescription}) = '$value'"
      })
        .toSeq
        .mkString("\n")

      s"${getClass.getSimpleName}: \n"
        .concat(optionsWithValues)
        .concat("\n")
    }
  }

  abstract class EngineConfig(val propertiesFile: String) extends BaseConfig

  case class BranchConfig(applicationBranch: String = "") extends BaseConfig {

    val scoptOptionMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.ApplicationBranch -> applicationBranch)
  }

  case class InitialLoadConfig(override val propertiesFile: String = "") extends EngineConfig(propertiesFile) {

    val scoptOptionMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile)
  }

  case class SourceLoadConfig(override val propertiesFile: String = "",
                              bancllName: String = "",
                              dtRiferimentoOpt: Option[String] = None,
                              versionNumberOpt: Option[String] = None) extends EngineConfig(propertiesFile) {

      val scoptOptionMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile,
        ScoptOption.Source -> bancllName,
        ScoptOption.DtRiferimento -> dtRiferimentoOpt.orNull,
        ScoptOption.VersionNumber -> versionNumberOpt.orNull)
  }

  case class ReloadConfig(override val propertiesFile: String = "",
                          mappingSpecificationFlag: Boolean = false,
                          lookUpFlag: Boolean = false,
                          completeOverwriteFlag: Boolean = false) extends EngineConfig(propertiesFile) {

      val scoptOptionMap: Map[ScoptOption.Value, Any] = Map(ScoptOption.PropertiesFile -> propertiesFile,
        ScoptOption.MappingSpecificationFlag -> mappingSpecificationFlag,
        ScoptOption.LookupSpecificationFlag -> lookUpFlag,
        ScoptOption.CompleteOverwriteFlag -> completeOverwriteFlag)
  }

  abstract class CustomParser[T] extends OptionParser[T]("scopt 3.3.0") {

    // Do not fail on unknown arguments and do not show warning
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

  }

  val branchParser: CustomParser[BranchConfig] = new CustomParser[BranchConfig] {

    opt[String](ScoptOption.ApplicationBranch.shortOption, ScoptOption.ApplicationBranch.longOption)
      .text(ScoptOption.ApplicationBranch.optionDescription)
      .required()
      .action((x, c) => c.copy(applicationBranch = x))
  }

  val initialLoadOptionParser: CustomParser[InitialLoadConfig] = new CustomParser[InitialLoadConfig] {

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.optionDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))
  }

  val sourceLoadOptionParser: CustomParser[SourceLoadConfig] = new CustomParser[SourceLoadConfig] {

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

  val reloadOptionParser: CustomParser[ReloadConfig] = new CustomParser[ReloadConfig] {

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
