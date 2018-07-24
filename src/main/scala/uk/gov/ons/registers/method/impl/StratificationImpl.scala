package uk.gov.ons.registers.method.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.model.CommonUnitFrameDataFields._
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields._

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) {
    /**
      *
      * @param sic07LowerRange
      * @param sic07UpperRange
      * @param payeEmployeesLowerRange
      * @param payeEmployeesUpperRange
      * @param cellNo
      * @return
      */
    def stratify1(sic07LowerRange: Int, sic07UpperRange: Int, payeEmployeesLowerRange: Long, payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] = {
      // TODO check if inclusive [Both  filters] - TEST
      val payeEmployeesAsIntField = s"temp_$paye_empees"
      val sic07AsLongField = s"temp_$sic07"
      val castedDf = frameDf
        .withColumn(colName = payeEmployeesAsIntField, frameDf.col(paye_empees).cast(LongType))
        .withColumn(colName = sic07AsLongField, frameDf.col(sic07).cast(IntegerType))

      castedDf
        .filter(castedDf(sic07AsLongField) >= sic07LowerRange && castedDf(sic07AsLongField) <= sic07UpperRange)
        .filter(castedDf(payeEmployeesAsIntField) >= payeEmployeesLowerRange && castedDf(payeEmployeesAsIntField) <= payeEmployeesUpperRange)
        .withColumn(cellNumber, lit(cellNo))
        .drop(payeEmployeesAsIntField, sic07AsLongField)
    }
  }
}
