package it.carloni.luca.aurora.spark.engine

import it.carloni.luca.aurora.spark.data.SpecificationRecord
import it.carloni.luca.aurora.utils.Utils.resolveDataType
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

class RawDataTransformerEngine {

  private final val logger = Logger.getLogger(getClass)

  def transformRawDataFrame(rawDataframe: DataFrame, specificationRecords: List[SpecificationRecord]): DataFrame = {

    val trustedColumns: Seq[Column] = specificationRecords
      .map((specificationRecord: SpecificationRecord) => {

        val rawColumnName: String = specificationRecord.colonna_rd
        val rawColumnCol: Column = col(rawColumnName)

        logger.info(s"Analyzing specification for raw column '$rawColumnName'")
        val functionsToApply: Seq[String] = Seq(specificationRecord.function_1, specificationRecord.function_2,
          specificationRecord.function_3, specificationRecord.function_4, specificationRecord.function_5)
          .filter(_.nonEmpty)
          .map(_.get)

        // IF THE COLUMN DOES NOT IMPLY ANY TRANSFORMATION BUT NEEDS TO BE KEPT
        if (functionsToApply.isEmpty && specificationRecord.flag_discard.isEmpty) {

          logger.info(s"No transformation to apply to raw column '$rawColumnName'")

          // CHECK IF INPUT AND OUTPUT DATATYPE MATCH
          val rawColumnType: String = specificationRecord.tipo_colonna_rd
          val trustedColumnType: String = specificationRecord.tipo_colonna_td
          val rawColumnColMaybeCasted: Column = if (rawColumnType.equalsIgnoreCase(trustedColumnType)) {

            logger.info(s"No type conversion to apply to raw column '$rawColumnName'. Raw type: '$rawColumnType', trusted type: '$trustedColumnType'")
            rawColumnCol

          } else {

            logger.info(s"Defining conversion for raw column '$rawColumnName' (from '$rawColumnType' to '$trustedColumnType')")
            rawColumnCol.cast(resolveDataType(trustedColumnType))
          }

          rawColumnColMaybeCasted.alias(specificationRecord.colonna_td)

        } else {

          // TODO: logica per concatenazione funzioni ETL
          rawColumnCol
        }
      })

    rawDataframe
  }
}
