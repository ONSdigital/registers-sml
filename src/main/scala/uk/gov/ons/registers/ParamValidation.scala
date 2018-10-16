package uk.gov.ons.registers

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.TransformDataFrames.filterByCellNumber
import uk.gov.ons.registers.Validation.ErrorMessage

object ParamValidation {
  private val MinimumSampleSize = 0
  private val LowerBoundPrnValue = BigDecimal(0)

  private def logWithErrorMsg[A](strataNumber: Int)(msg: A): Option[Nothing] = {
    val logErrorMsg = s"Could not process strata ($strataNumber): $msg"
    Patch.log(level = "warn", msg = logErrorMsg)
    None
  }

  private def validatePrnStartPoint(strataNumber: Int, startingPrn: BigDecimal): Validation[ErrorMessage, BigDecimal] = {
    val max = BigDecimal(1)
    if (startingPrn > LowerBoundPrnValue && startingPrn < max) Success(valid = startingPrn)
    else Failure(s"Error: Prn start point [$startingPrn] must be a decimal no smaller than $LowerBoundPrnValue and greater than $max")
  }

  private def validateSampleSize(strataNumber: Int, maxSize: Int, sampleSize: Int): Validation[ErrorMessage, Int] =
    if (sampleSize > maxSize) {
      logWithErrorMsg(strataNumber)(msg = s"Error: Sample size [$sampleSize] must be a natural number less than $maxSize. " +
        s"Parameter overridden with with max sample size [$maxSize]")
      Success(valid = maxSize)
    } else if (sampleSize > MinimumSampleSize && sampleSize <= maxSize) Success(valid = sampleSize)
    else Failure(s"Error: Sample size [$sampleSize] must be a natural number greater than $MinimumSampleSize and less than $maxSize")

  def validate(inputDF: DataFrame, strataNumber: Int, startingPrn: BigDecimal, sampleSize: Int): Option[Int] =
    Validation.toOption(
      validatePrnStartPoint(strataNumber, startingPrn),
      validateSampleSize(strataNumber, filterByCellNumber(inputDF)(strataNumber).count.toInt, sampleSize)
    )(onFailure = logWithErrorMsg(strataNumber) _, onSuccess = (_, sampleSize) => Some(sampleSize))
}
