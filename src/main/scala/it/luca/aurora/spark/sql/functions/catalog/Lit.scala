package it.luca.aurora.spark.sql.functions.catalog

import it.luca.aurora.spark.sql.functions.common.StaticColumnExpression
import it.luca.aurora.spark.sql.functions.common.ColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

case class Lit(override val stringExpression: String)
  extends StaticColumnExpression(stringExpression, ColumnExpression.Lit) {

  final val litValue: String = group(2)

  override def asString: String = s"${functionName.toUpperCase}($litValue)"

  override def getColumn: Column = {

    val outputLitValue: Any = if (litValue.startsWith("'") & litValue.endsWith("'")) {

      // If the literal value is a string, first and last characters must be omitted
      litValue.slice(1, litValue.length - 1)

    } else {

      // Otherwise, if the literal value is a number, make it int or double depending on the presence of a dot
      if (litValue.contains(".")) litValue.toDouble else litValue.toInt
    }

    lit(outputLitValue)
  }
}
