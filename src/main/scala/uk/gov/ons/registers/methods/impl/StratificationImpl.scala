package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.Codes
import uk.gov.ons.registers.methods.Stratification
import uk.gov.ons.registers.model.CommonFrameDataFields.{prn, sic07}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) extends Stratification {
    /**
      * USAGE: Stratify a Frame
      *
      * @param cellNo - to denote the strata to which it is allocated
      * @return Frame - A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      */
    def stratify1(sic07LowerClass: Int, sic07UpperClass: Int, payeEmployeesLowerRange: Long,
      payeEmployeesUpperRange: Long, cellNo: Int, bounds: String): Dataset[Row] = {
      frameDf
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .filter(frameDf(bounds) >= payeEmployeesLowerRange &&
          frameDf(bounds) <= payeEmployeesUpperRange)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(cellNo))
    }
    /**
      * USAGE: Extracting strata and filtering for units that contain a null in the `paye_empees` - then labelling with -2
      *         [PATCH]
      *
      * @param strata -  A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      * @return {DataFrame} - a combined dataframe of the strata followed by any units with payeEmployee as null
      */
    def postPayeEmployeeNullDenotation1(strata: DataFrame, sic07LowerClass: Int, sic07UpperClass: Int, bounds: String): Dataset[Row] = {
      val rawUnits = strata
        .drop(StratificationPropertiesFields.cellNumber)

      val nullPayeEmployeeUnits = frameDf
        .except(rawUnits)
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .where(frameDf(bounds).isNull)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(Codes.BoundsNullCode))
        .orderBy(prn)

      strata
        .union(nullPayeEmployeeUnits)
    }

    /**
      * USAGE: A post step of Stratification, where any unallocated unit is flagged with the error code (-1)
      *
      * @param strataAllocatedDataFrame - a Stratified Frame
      * @return Frame - A DataSet, maintains all the stratas compiled, but includes all units unallocated
      */
    def postStratification1(strataAllocatedDataFrame: DataFrame): Dataset[Row] = {
      val allocatedWithCellNumField = strataAllocatedDataFrame
        .drop(StratificationPropertiesFields.cellNumber)
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
