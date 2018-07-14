package uk.gov.ons.registers.method.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes

import uk.gov.ons.registers.models.CommonUnitFrameDataFields.prn
import uk.gov.ons.registers.models.StratificationProperties.cellNumber


object SampleImpl {

  implicit class SampleMethodsImpl(inputDataDF: DataFrame) {
    /**
      * USAGE: PRN Sampling (P)
      *
      * @note  The omitted (by filter expression) records is appended to the resulting filtered dataframe.
      * @param startPoint - splitting point of input dataframe [Stratification Property]
      * @param sampleSize - number of records to return [Stratification Property]
      * @param cellNo - appended value to each record per request [Stratification Property]
      * @return a dataframe of size defined by sampleSize and is sequentially after the given startpoint - looping
      *         around the dataframe if needed
      */
    def sample1(startPoint: BigDecimal, sampleSize: Int, cellNo: Int): DataFrame = {
      val filtered = inputDataDF
        .withColumn(prn, inputDataDF.col(prn).cast(DataTypes.createDecimalType()))
        .orderBy(prn)
        .filter(inputDataDF(prn) >= startPoint)

      val remainder = inputDataDF
        .except(filtered)

      filtered
        .union(remainder)
        .limit(sampleSize)
        .withColumn(cellNumber, lit(cellNo))
    }

    /**
      * USAGE: Census (C)
      *
      * @param cellNo number appended to each row of result to correspond to request
      * @return a dataframe with all records for census
      */
    def sample1(cellNo: Int): DataFrame =
      inputDataDF
        .withColumn(cellNumber, lit(cellNo))
  }
}
