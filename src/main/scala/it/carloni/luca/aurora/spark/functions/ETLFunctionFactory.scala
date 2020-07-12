package it.carloni.luca.aurora.spark.functions

object ETLFunctionFactory {

  def apply[T <: ETLFunction](functionToApply: String): ETLFunction = {

    // FILTER OUT LOOK_UP SIGNATURES
    val matchingSignatures: Signatures.ValueSet = Signatures.values
      .filter(_.regex
        .findFirstMatchIn(functionToApply)
        .nonEmpty)

    // IF A FUNCTION MATCHES
    if (matchingSignatures.nonEmpty) {

      // RETRIEVE IT
      matchingSignatures.head match {

        case Signatures.dateFormat => DateFormatFunction(functionToApply)
        case Signatures.lpad => LpadFunction(functionToApply)
        case Signatures.rpad => RpadFunction(functionToApply)
        case Signatures.toDate => ToDateFunction(functionToApply)
        case Signatures.toTimestamp => ToTimestampFunction(functionToApply)
      }
    } else {

      // TODO: eccezione
      throw new Exception
    }
  }
}
