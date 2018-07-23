package uk.gov.ons.registers.method.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

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
    def stratify1(sic07LowerRange: Int, sic07UpperRange: Int, payeEmployeesLowerRange: Long, payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] =
      // TODO check if inclusive [Both  filters] - TEST
      frameDf
        .filter(frameDf(lowerClassSIC07) >= sic07LowerRange && frameDf(upperClassSIC07) <= sic07UpperRange)
        .filter(frameDf(lowerSizePayeEmployee) >= payeEmployeesLowerRange && frameDf(upperSizePayeEmployee) <= payeEmployeesUpperRange)
        .withColumn(cellNumber, lit(cellNo))
  }
}
