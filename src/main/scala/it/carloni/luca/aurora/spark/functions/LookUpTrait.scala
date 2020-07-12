package it.carloni.luca.aurora.spark.functions

import org.apache.spark.sql.{Column, DataFrame}

trait LookUpTrait {

  def transform(inputColumn: Column, lookUpDataFrame: DataFrame): Column

}
