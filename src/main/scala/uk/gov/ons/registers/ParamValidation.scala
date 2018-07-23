package uk.gov.ons.registers


object ParamValidation {
  private val min = 0
  private type ErrorMessage = String

  private def validatePrnStartPoint(strataNumber: Int, startingPrn: BigDecimal): Validation[BigDecimal, Nothing, ErrorMessage] = {
    val max = 1
    if (startingPrn > min && startingPrn < max) { Success(optA = Some(startingPrn)) }
    else Failure(s"Error: Prn start point [$startingPrn] must be a decimal no smaller than $min and greater than $max")
  }

  private def validateSampleSize(strataNumber: Int, maxSize: Int, sampleSize: Int): Validation[Nothing, Int, ErrorMessage] =
    if (sampleSize > maxSize) {
      logWithErrorMsg(strataNumber)(msg = s"Error: Sample size [$sampleSize] must be a natural number less than $maxSize. " +
        s"Parameter overridden with with max sample size [$maxSize]")
      Success(optB = Some(maxSize)) }
    else if (sampleSize > min && sampleSize < maxSize) Success(optB = Some(sampleSize))
    else Failure(s"Error: Sample size [$sampleSize] must be a natural number greater than $min and less than $maxSize")


  private def logWithErrorMsg[A](strataNumber: Int)(msg: A): Option[Nothing] = {
    val logErrorMsg = s"[WARN] Could not process strata ($strataNumber): $msg"
    // TODO - REPLACE AND ADD LOGGER
    println(logErrorMsg)
    None
  }

  def validate(maxSize: Int, strataNumber: Int, startingPrn: BigDecimal, sampleSize: Int): Option[Int] =
    Validation.mapOption(validatePrnStartPoint(strataNumber, startingPrn),
      validateSampleSize(strataNumber, maxSize, sampleSize), strataNumber)(logWithErrorMsg)

}
