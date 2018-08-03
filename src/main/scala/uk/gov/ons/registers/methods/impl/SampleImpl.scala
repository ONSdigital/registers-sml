package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes

import uk.gov.ons.registers.model.CommonUnitFrameDataFields.prn
import uk.gov.ons.registers.model.stratification.PrnStatisticalProperty.{precision, scale}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.cellNumber

object SampleImpl {

  implicit class SampleMethodsImpl(inputDataDF: DataFrame) {
    /**
      * USAGE: PRN Sampling (P)
      *
      * @note  The omitted (by filter expression) rows is appended to the resulting filtered DataFrame.
      * @param startPoint - splitting point of frame DataFrame [Stratification Property]
      * @param sampleSize - number of frame rows to return [Stratification Property]
      * @param cellNo - appended value to each row per strata [Stratification Property]
      * @return a DataFrame of size defined by sampleSize and is sequentially after the given start point - looping
      *         around the DataFrame if needed
      */
    def sample1(startPoint: BigDecimal, sampleSize: Int, cellNo: Int): DataFrame = {
      val prnAsBigDecimal = s"${prn}_temp"
      val inputDataWithBigDecimal = inputDataDF
        .withColumn(prnAsBigDecimal, inputDataDF.col(prn).cast(DataTypes.createDecimalType(precision, scale)))

      val filtered = inputDataWithBigDecimal
        .orderBy(prnAsBigDecimal)
        .filter(inputDataWithBigDecimal(prnAsBigDecimal) >= startPoint)
        .drop(prnAsBigDecimal)

      val remainder = inputDataDF
        .except(filtered)

      filtered
        .union(remainder)
        .limit(sampleSize)
        .withColumn(cellNumber, lit(cellNo))
    }

    /**
      * USAGE: PRN Sampling (P)
      *
      * @note  The omitted (by filter expression) rows is appended to the resulting filtered DataFrame. As a result
      *        given a high starting point or a large enough sample size the selection of a Sample can loop the
      *        DataFrame sequentially.
      * @param startPoint - splitting point of frame DataFrame [Stratification Property]
      * @param sampleSize - number of frame rows to return [Stratification Property]
      * @param cellNo - appended value to each row per strata [Stratification Property]
      * @return a DataFrame of a subset population of Stratified Frame defined by Sample Size all of which with the
      *         expected Cell Number.
      */
    def sample2(startPoint: BigDecimal, sampleSize: Int, cellNo: Int): DataFrame = {
      val strata = inputDataDF
        .filter(_.getAs[String](cellNumber).toInt == cellNo)

      val prnAsBigDecimal = s"${prn}_temp"
      val inputDataWithBigDecimal = strata
        .withColumn(prnAsBigDecimal, strata.col(prn).cast(DataTypes.createDecimalType(precision, scale)))

      val filtered = inputDataWithBigDecimal
        .orderBy(prnAsBigDecimal)
        .filter(inputDataWithBigDecimal(prnAsBigDecimal) >= startPoint)

      val remainder = inputDataWithBigDecimal
        .except(filtered)
        .orderBy(prnAsBigDecimal)

      filtered
        .union(remainder)
        .limit(sampleSize)
        .drop(prnAsBigDecimal)
    }

    /**
      * USAGE: Census (C)
      * @note This assume there is no Cell Number on strata - in fact no stratification is
      *       pre-done.
      *
      * @param cellNo number appended to each frame row of sample to correspond to strata
      * @return a DataFrame with all records for census
      */
    def sample1(cellNo: Int): DataFrame =
      inputDataDF
        .withColumn(cellNumber, lit(cellNo))


    /**
      * USAGE: Census (C)
      *
      * @param cellNo search and identify the strata by
      * @return the entire population for the given strata
      */
    def sample2(cellNo: Int): DataFrame =
      inputDataDF
        .filter(_.getAs[String](cellNumber).toInt == cellNo)
  }
}
