package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.Codes
import uk.gov.ons.registers.model.CommonFrameDataFields.{cellNumber, payeEmployees, prn, sic07}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) {
    /**
      * USAGE: Stratified a Frame
      *
      * @param cellNo - to denote the strata to which it is allocated
      * @return Frame - A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      */
    def stratify1(sic07LowerClass: Int, sic07UpperClass: Int, payeEmployeesLowerRange: Long,
      payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] =
      frameDf
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .filter(frameDf(payeEmployees) >= payeEmployeesLowerRange &&
          frameDf(payeEmployees) <= payeEmployeesUpperRange)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(cellNo))

    /**
      * USAGE: A post step of Stratification, where any unallocated unit is flagged with the error code (-1)
      *
      * @param strataAllocatedDataFrame - a Stratified Frame
      * @return Frame - A DataSet, maintains all the stratas compiled, but includes all units unallocated
      */
    def postStratification1(strataAllocatedDataFrame: DataFrame): Dataset[Row] = {
      val allocatedWithCellNumField = strataAllocatedDataFrame
        .drop(cellNumber)
        .distinct

      val unallocated = frameDf
        .except(allocatedWithCellNumField)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(Codes.ErrorCode))
        .orderBy(prn)

      strataAllocatedDataFrame
        .union(unallocated)
    }
  }
}
