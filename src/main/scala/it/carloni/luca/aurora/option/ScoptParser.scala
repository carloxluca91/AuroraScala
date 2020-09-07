package it.carloni.luca.aurora.option

import java.time.LocalDate

import it.carloni.luca.aurora.utils.DateFormat
import scopt.OptionParser

import scala.util.Try

object ScoptParser {

  private def objectToString(tClass: Class[_], scoptOptionsMap: Map[ScoptOption, Any]): String = {

    val optionsWithValues: String = (scoptOptionsMap map { case (key, value) =>  s"\t-${key.getShortOption}, " +
      s"--${key.getLongOption} (${key.getDescription}) = '$value'"})
      .toSeq
      .mkString("\n")

    s"${tClass.getSimpleName}: \n"
      .concat(optionsWithValues)
      .concat("\n")
  }

  case class BranchConfig(applicationBranch: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption, Any] = Map(ScoptOption.APPLICATION_BRANCH -> applicationBranch)
      objectToString(getClass, toStringMap)
    }
  }

  case class InitialLoadConfig(propertiesFile: String = "") {

    override def toString: String = {

      val toStringMap: Map[ScoptOption, Any] = Map(ScoptOption.PROPERTIES_OPTION -> propertiesFile)
      objectToString(getClass, toStringMap)
    }
  }

  case class SourceLoadConfig(propertiesFile: String = "",
                              bancllName: String = "",
                              dtRiferimentoOpt: Option[String] = None,
                              versionNumberOpt: Option[Double] = None) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption, Any] = Map(ScoptOption.PROPERTIES_OPTION -> propertiesFile,
        ScoptOption.SOURCE -> bancllName,
        ScoptOption.DT_RIFERIMENTO -> dtRiferimentoOpt.orNull,
        ScoptOption.VERSION_NUMBER -> versionNumberOpt.orNull)
      objectToString(getClass, toStringMap)
    }
  }

  case class ReloadConfig(propertiesFile: String = "",
                          mappingSpecificationFlag: Boolean = false,
                          lookUpFlag: Boolean = false,
                          completeOverwriteFlag: Boolean = false) {

    override def toString: String = {

      val toStringMap: Map[ScoptOption, Any] = Map(ScoptOption.PROPERTIES_OPTION -> propertiesFile,
        ScoptOption.MAPPING_SPECIFICATION_FLAG -> mappingSpecificationFlag,
        ScoptOption.LOOKUP_SPECIFICATION_FLAG -> lookUpFlag,
        ScoptOption.COMPLETE_OVERWRITE_FLAG -> completeOverwriteFlag)
      objectToString(getClass, toStringMap)
    }
  }

  private final val scoptProgramName: String = "scopt 3.3.0"

  val branchParser: OptionParser[BranchConfig] = new OptionParser[BranchConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.APPLICATION_BRANCH.getShortOption, ScoptOption.APPLICATION_BRANCH.getLongOption)
      .text(ScoptOption.APPLICATION_BRANCH.getDescription)
      .required()
      .action((x, c) => c.copy(applicationBranch = x))
  }

  val initialLoadOptionParser: OptionParser[InitialLoadConfig] = new OptionParser[InitialLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PROPERTIES_OPTION.getShortOption, ScoptOption.PROPERTIES_OPTION.getLongOption)
      .text(ScoptOption.PROPERTIES_OPTION.getDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))
  }

  val sourceLoadOptionParser: OptionParser[SourceLoadConfig] = new OptionParser[SourceLoadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PROPERTIES_OPTION.getShortOption, ScoptOption.PROPERTIES_OPTION.getLongOption)
      .text(ScoptOption.PROPERTIES_OPTION.getDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[String](ScoptOption.SOURCE.getShortOption, ScoptOption.SOURCE.getLongOption)
      .text(ScoptOption.SOURCE.getDescription)
      .required()
      .action((x, c) => c.copy(bancllName = x))

    opt[String](ScoptOption.DT_RIFERIMENTO.getShortOption, ScoptOption.DT_RIFERIMENTO.getLongOption)
      .text(ScoptOption.DT_RIFERIMENTO.getDescription)
      .validate(inputDate => {

        val tryParseDtRiferimento: Try[LocalDate] = Try(LocalDate.parse(inputDate, DateFormat.DT_RIFERIMENTO.getFormatter))
        if (tryParseDtRiferimento.isSuccess) success
        else failure(s"Cannot parse business date. Provided '$inputDate', should follow format '${DateFormat.DT_RIFERIMENTO.getFormat}'")
      })
      .action((x, c) => c.copy(dtRiferimentoOpt = Some(x)))

    opt[Double](ScoptOption.VERSION_NUMBER.getShortOption, ScoptOption.VERSION_NUMBER.getLongOption)
      .text(ScoptOption.VERSION_NUMBER.getDescription)
      .action((x, c) => c.copy(versionNumberOpt = Some(x)))
  }

  val reloadOptionParser: OptionParser[ReloadConfig] = new OptionParser[ReloadConfig](scoptProgramName) {

    // DO NOT FAIL ON UNKNOWN ARGUMENTS AND DO NOT SHOW WARNING
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

    opt[String](ScoptOption.PROPERTIES_OPTION.getShortOption, ScoptOption.PROPERTIES_OPTION.getLongOption)
      .text(ScoptOption.PROPERTIES_OPTION.getDescription)
      .required()
      .action((x, c) => c.copy(propertiesFile = x))

    opt[Unit](ScoptOption.MAPPING_SPECIFICATION_FLAG.getShortOption, ScoptOption.MAPPING_SPECIFICATION_FLAG.getLongOption)
      .text(ScoptOption.MAPPING_SPECIFICATION_FLAG.getDescription)
      .action((_, c) => c.copy(mappingSpecificationFlag = true))

    opt[Unit](ScoptOption.LOOKUP_SPECIFICATION_FLAG.getShortOption, ScoptOption.LOOKUP_SPECIFICATION_FLAG.getLongOption)
      .text(ScoptOption.LOOKUP_SPECIFICATION_FLAG.getDescription)
      .action((_, c) => c.copy(lookUpFlag = true))

    opt[Unit](ScoptOption.COMPLETE_OVERWRITE_FLAG.getShortOption, ScoptOption.COMPLETE_OVERWRITE_FLAG.getLongOption)
      .text(ScoptOption.COMPLETE_OVERWRITE_FLAG.getDescription)
      .action((_, c) => c.copy(completeOverwriteFlag = true))
  }
}
