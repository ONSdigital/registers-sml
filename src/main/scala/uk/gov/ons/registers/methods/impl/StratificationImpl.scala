package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import uk.gov.ons.registers.model.CommonUnitFrameDataFields._
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields
import uk.gov.ons.registers.model.stratification.PrnStatisticalProperty.{precision, scale}

object StratificationImpl {
  implicit class StratificationMethodsImpl(frameDf: DataFrame) {
    /**
      * USAGE:
      *
      * @param sic07LowerClass
      * @param sic07UpperClass
      * @param payeEmployeesLowerRange
      * @param payeEmployeesUpperRange
      * @param cellNo
      * @return
      */
    def stratify1(sic07LowerClass: Int, sic07UpperClass: Int, payeEmployeesLowerRange: Long, payeEmployeesUpperRange: Long, cellNo: Int): Dataset[Row] = {
      val payeEmployeesAsIntField = s"temp_$payeEmployees"
      val sic07AsLongField = s"temp_$sic07"
      val castedDf = frameDf
        .withColumn(colName = payeEmployeesAsIntField, frameDf.col(payeEmployees).cast(LongType))
        .withColumn(colName = sic07AsLongField, frameDf.col(sic07).cast(IntegerType))

      castedDf
        .filter(castedDf(sic07AsLongField) >= sic07LowerClass && castedDf(sic07AsLongField) <= sic07UpperClass)
        .filter(castedDf(payeEmployeesAsIntField) >= payeEmployeesLowerRange && castedDf(payeEmployeesAsIntField) <= payeEmployeesUpperRange)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(cellNo.toString))
        .drop(payeEmployeesAsIntField, sic07AsLongField)
    }

    /**
      * USAGE:
      *
      * @param strataAllocatedDataFrame
      * @return
      */
    def postStratification1(strataAllocatedDataFrame: DataFrame): Dataset[Row] = {
      val prnAsLongField = s"temp_$prn"
      val allocatedWithCellNumField = strataAllocatedDataFrame
        .drop(cellNumber)
        .distinct

      val unallocated = frameDf
        .except(allocatedWithCellNumField)
        .withColumn(StratificationPropertiesFields.cellNumber, lit(literal = "-1"))
        .withColumn(colName = prnAsLongField, col = frameDf.col(prn).cast(DataTypes.createDecimalType(precision, scale)))
        .orderBy(prnAsLongField)
        .drop(prnAsLongField)

      strataAllocatedDataFrame
        .union(unallocated)
    }
  }
}
