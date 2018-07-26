package uk.gov.ons.registers


object ParamValidation {
  private val lowerBoundLimit = 0
  private type ErrorMessage = String

  private def validatePrnStartPoint(strataNumber: Int, startingPrn: BigDecimal): Validation[BigDecimal, ErrorMessage] = {
    val max = BigDecimal(1L)
    val thisMin = BigDecimal(lowerBoundLimit.toLong)
    if (startingPrn > thisMin && startingPrn < max) Success(valid = startingPrn)
    else Failure(s"Error: Prn start point [$startingPrn] must be a decimal no smaller than $thisMin and greater than $max")
  }

  private def validateSampleSize(strataNumber: Int, maxSize: Int, sampleSize: Int): Validation[Int, ErrorMessage] =
    if (sampleSize > maxSize) {
      logWithErrorMsg(strataNumber)(msg = s"Error: Sample size [$sampleSize] must be a natural number less than $maxSize. " +
        s"Parameter overridden with with max sample size [$maxSize]")
      Success(valid = maxSize)
    } else if (sampleSize > lowerBoundLimit && sampleSize <= maxSize) Success(valid = sampleSize)
    else Failure(s"Error: Sample size [$sampleSize] must be a natural number greater than $lowerBoundLimit and less than $maxSize")


  private def logWithErrorMsg[A](strataNumber: Int)(msg: A): Option[Nothing] = {
    val logErrorMsg = s"[WARN] Could not process strata ($strataNumber): $msg"
    // TODO - REPLACE AND ADD LOGGER
    println(logErrorMsg)
    None
  }

  def validate(maxSize: Int, strataNumber: Int, startingPrn: BigDecimal, sampleSize: Int): Option[Int] =
    Validation.toOption(
      validatePrnStartPoint(strataNumber, startingPrn),
      validateSampleSize(strataNumber, maxSize, sampleSize)
    )(onFailure = logWithErrorMsg(strataNumber) _, onSuccess = (_, sampleSize) => Some(sampleSize))
}
