package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.Codes
import uk.gov.ons.registers.model.CommonUnitFrameDataFields.{cellNumber, payeEmployees, prn, sic07}
import uk.gov.ons.registers.model.stratification.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) {
    /**
      * USAGE: Stratified a Frame
      *
      * @param cellNo - to denote the strata to which it is allocated
      * @return Frame - A DataSet that has strata(s) composed from filtering units by sic07 and Paye Employment range
      */
    def stratify1(sic07LowerClass: Int, sic07UpperClass: Int, payeEmployeesLowerRange: Long,
      payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] = {
      val payeEmployeesAsIntField = s"temp_$payeEmployees"
      val sic07AsLongField = s"temp_$sic07"
      val castedDf = frameDf
        .withColumn(colName = payeEmployeesAsIntField, frameDf.col(payeEmployees).cast(LongType))
        .withColumn(colName = sic07AsLongField, frameDf.col(sic07).cast(IntegerType))

      castedDf
        .filter(castedDf(sic07AsLongField) >= sic07LowerClass && castedDf(sic07AsLongField) <= sic07UpperClass)
        .filter(castedDf(payeEmployeesAsIntField) >= payeEmployeesLowerRange &&
          castedDf(payeEmployeesAsIntField) <= payeEmployeesUpperRange)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(cellNo.toString))
        .drop(payeEmployeesAsIntField, sic07AsLongField)
    }

    /**
      * USAGE: A post step of Stratification, where any unallocated unit is flagged with the error code (-1)
      *
      * @param strataAllocatedDataFrame - a Stratified Frame
      * @return Frame - A DataSet, maintains all the stratas compiled, but includes all units unallocated
      */
    def postStratification1(strataAllocatedDataFrame: DataFrame): Dataset[Row] = {
      val prnAsLongField = s"temp_$prn"
      val allocatedWithCellNumField = strataAllocatedDataFrame
        .drop(cellNumber)
        .distinct

      val unallocated = frameDf
        .except(allocatedWithCellNumField)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(Codes.ErrorCode))
        .withColumn(colName = prnAsLongField, col = frameDf.col(prn).cast(DataTypes.createDecimalType(precision, scale)))
        .orderBy(prnAsLongField)
        .drop(prnAsLongField)

      strataAllocatedDataFrame
        .union(unallocated)
    }
  }
}
