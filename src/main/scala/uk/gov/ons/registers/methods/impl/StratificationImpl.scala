package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.Codes
import uk.gov.ons.registers.model.CommonFrameDataFields.{cellNumber, payeEmployees, prn, sic07}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) {
    /**
      * USAGE: Stratify a Frame
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
      * USAGE: Stratify a Frame
      *
      * Included with the strata are units that contain a null in the `paye_empees` field - labelled with -2
      *
      * @param cellNo - to denote the strata to which it is allocated
      * @return Frame - A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      */
    @deprecated("Migrate to postWithPayeEmployeeNullDenotation()", "feature/support-paye-emp-nulls - 14 Sept 2018")
    def stratify2(sic07LowerClass: Int, sic07UpperClass: Int, payeEmployeesLowerRange: Long,
                  payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] = {
      val strata = frameDf
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .filter(frameDf(payeEmployees) >= payeEmployeesLowerRange &&
          frameDf(payeEmployees) <= payeEmployeesUpperRange)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(cellNo))

      val nullPayeEmployeeUnits = frameDf
        .except(strata)
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .where(frameDf(payeEmployees).isNull) // ????
        .withColumn(StratificationPropertiesFields.cellNumber, lit(Codes.PayeEmployeeNullCode))
        .orderBy(prn)

      strata
        .union(nullPayeEmployeeUnits)
    }

    /**
      * USAGE: Stratify a Frame
      *
      * Extracts strata and filters units that contain a null in the `paye_empees` field (labelled with -2)
      * Then combines both
      *
      * @param strata -  A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      * @return
      */
    def postWithPayeEmployeeNullDenotation(strata: DataFrame, sic07LowerClass: Int, sic07UpperClass: Int): Dataset[Row] = {
      val nullPayeEmployeeUnits = frameDf
        .except(strata)
        .filter(frameDf(sic07) >= sic07LowerClass && frameDf(sic07) <= sic07UpperClass)
        .where(frameDf(payeEmployees).isNull) // ????
        .withColumn(StratificationPropertiesFields.cellNumber, lit(Codes.PayeEmployeeNullCode))
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
