package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.spark.data.SpecificationRecord
import it.carloni.luca.aurora.spark.functions._
import it.carloni.luca.aurora.utils.Utils.resolveDataType
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

class RawDataTransformerEngine(private final val lookUpDataFrame: DataFrame) {

  private final val logger = Logger.getLogger(getClass)

  def transformRawDataFrame(rawDataframe: DataFrame, specificationRecords: List[SpecificationRecord]): DataFrame = {

    val trustedColumns: Seq[Column] = specificationRecords
      .map((specificationRecord: SpecificationRecord) => {

        val rawColumnName: String = specificationRecord.colonna_rd

        logger.info(s"Analyzing specification for raw column '$rawColumnName'")
        val functionsToApply: Seq[String] = Seq(specificationRecord.function_1, specificationRecord.function_2,
          specificationRecord.function_3, specificationRecord.function_4, specificationRecord.function_5)
          .filter(_.nonEmpty)
          .map(_.get)

        // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT MUST TO BE KEPT
        if (functionsToApply.isEmpty && specificationRecord.flag_discard.isEmpty) {

          logger.info(s"No transformation to apply to raw column '$rawColumnName'")

          // CHECK IF INPUT AND OUTPUT DATATYPE MATCH
          val rawColumnType: String = specificationRecord.tipo_colonna_rd
          val trustedColumnType: String = specificationRecord.tipo_colonna_td
          val rawColumnCol: Column = if (rawColumnType.equalsIgnoreCase(trustedColumnType)) {

            logger.info(s"No type conversion to apply to raw column '$rawColumnName'. Raw type: '$rawColumnType', trusted type: '$trustedColumnType'")
            col(rawColumnName)

          } else {

            logger.info(s"Defining conversion for raw column '$rawColumnName' (from '$rawColumnType' to '$trustedColumnType')")
            col(rawColumnName).cast(resolveDataType(trustedColumnType))
          }

          rawColumnCol.alias(specificationRecord.colonna_td)

        } else {

          //TODO: applicazione funzioni
          RawDataTransformerEngine(col(rawColumnName), "aa", lookUpDataFrame)
        }
      })

    rawDataframe
  }

}

object RawDataTransformerEngine {

  private final val logger = Logger.getLogger(getClass)

  def apply(column: Column, functionToApply: String, lookUpDataFrame: DataFrame): Column = {

    val matchingSignatures: Signatures.ValueSet = Signatures.values
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      val matchingSignature: Signatures.Value = matchingSignatures.head
      matchingSignature match {

        case Signatures.dateFormat => new DateFormatFunction(column, functionToApply).transform
        case Signatures.lpad => new LpadFunction(column, functionToApply).transform
        case Signatures.lookUp => new LookupFunction(column, functionToApply, lookUpDataFrame).transform
        case Signatures.rpad => new RpadFunction(column, functionToApply).transform
        case Signatures.toDate => new ToDateFunction(column, functionToApply).transform
        case Signatures.toTimestamp => new ToTimestampFunction(column, functionToApply).transform
      }

    } else {

      // TODO: lancio eccezione personalizzata
      logger.error(s"Unable to match such function: $functionToApply")
      throw new Exception
    }
  }
}
