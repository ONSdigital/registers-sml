package uk.gov.ons.registers.methods.impl

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.TransformDataFrames.filterByCellNumber
import uk.gov.ons.registers.model.CommonUnitFrameDataFields.prn

object SampleImpl {
  implicit class SampleMethodsImpl(inputDataDF: DataFrame) {
    /**
      * USAGE: PRN Sampling (P)
      *
      * @note The omitted (by filter expression) rows is appended to the resulting filtered DataFrame. As a result
      *       given a high starting point or a large enough sample size the selection of a Sample can loop the
      *       DataFrame sequentially.
      * @param startPoint - splitting point of frame DataFrame [Stratification Property]
      * @param sampleSize - number of frame rows to return [Stratification Property]
      * @param cellNo     - appended value to each row per strata [Stratification Property]
      * @return a DataFrame of a subset population of Stratified Frame defined by Sample Size all of which with the
      *         expected Cell Number.
      */
    def sample1(startPoint: BigDecimal, sampleSize: Int, cellNo: Int): DataFrame = {
      val strata = filterByCellNumber(inputDataDF)(cellNo)

      val filtered = strata
        .orderBy(prn)
        .filter(strata(prn) >= startPoint)

      val remainder = strata
        .except(filtered)
        .orderBy(prn)

      filtered
        .union(remainder)
        .limit(sampleSize)
    }

    /**
      * USAGE: Census (C)
      *
      * @param cellNo search and identify the strata by
      * @return the entire population for the given strata
      */
    def sample1(cellNo: Int): DataFrame =
      filterByCellNumber(inputDataDF)(cellNo)
  }
}
