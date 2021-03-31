package it.luca.aurora.option

import it.luca.aurora.enumeration.{Branch, ScoptOption}
import scopt.OptionParser

import java.text.SimpleDateFormat
import scala.util.Try

object ScoptParser {

  abstract class CustomParser[T] extends OptionParser[T]("scopt 3.3.0") {

    // Do not fail on unknown arguments and do not show warning
    override def errorOnUnknownArgument = false

    override def reportWarning(msg: String): Unit = {}

  }

  val branchParser: CustomParser[BranchConfig] = new CustomParser[BranchConfig] {

    opt[String](ScoptOption.ApplicationBranch.shortOption, ScoptOption.ApplicationBranch.longOption)
      .text(ScoptOption.ApplicationBranch.optionDescription)
      .required()
      .validate(x => {
        if (Branch.exists(x)) success
        else failure(s"Illegal branch $x")
      })
      .action((x, c) => c.copy(applicationBranch = x))

    opt[String](ScoptOption.PropertiesFile.shortOption, ScoptOption.PropertiesFile.longOption)
      .text(ScoptOption.PropertiesFile.optionDescription)
      .required()
      .validate(x => {
        if (x.endsWith(".properties")) success
        else failure(s"Illegal file ($x). It must have .properties extension")
      })
      .action((x, c) => c.copy(propertiesFile = x))
  }

  val sourceLoadOptionParser: CustomParser[DataSourceLoadConfig] = new CustomParser[DataSourceLoadConfig] {

    opt[String](ScoptOption.DataSource.shortOption, ScoptOption.DataSource.longOption)
      .text(ScoptOption.DataSource.optionDescription)
      .required()
      .action((x, c) => c.copy(dataSource = x))

    opt[String](ScoptOption.DtBusinessDate.shortOption, ScoptOption.DtBusinessDate.longOption)
      .text(ScoptOption.DtBusinessDate.optionDescription)
      .validate(x => {
        if (Try { new SimpleDateFormat("yyyy-MM-dd").parse(x) }
          .isSuccess) success
        else failure(s"Cannot parse business date. Provided '$x', should follow format yyyy-MM-dd")
      })
      .action((x, c) => c.copy(dtBusinessDate = Some(x)))

    opt[String](ScoptOption.SpecificationVersion.shortOption, ScoptOption.SpecificationVersion.longOption)
      .text(ScoptOption.SpecificationVersion.optionDescription)
      .action((x, c) => c.copy(specificationVersion = Some(x)))
  }

  val reloadOptionParser: CustomParser[ReloadConfig] = new CustomParser[ReloadConfig] {

    opt[Unit](ScoptOption.MappingSpecificationFlag.shortOption, ScoptOption.MappingSpecificationFlag.longOption)
      .text(ScoptOption.MappingSpecificationFlag.optionDescription)
      .action((_, c) => c.copy(specificationFlag = true))

    opt[Unit](ScoptOption.LookupSpecificationFlag.shortOption, ScoptOption.LookupSpecificationFlag.longOption)
      .text(ScoptOption.LookupSpecificationFlag.optionDescription)
      .action((_, c) => c.copy(lookUpFlag = true))

    opt[Unit](ScoptOption.CompleteOverwriteFlag.shortOption, ScoptOption.CompleteOverwriteFlag.longOption)
      .text(ScoptOption.CompleteOverwriteFlag.optionDescription)
      .action((_, c) => c.copy(completeOverwriteFlag = true))
  }
}
