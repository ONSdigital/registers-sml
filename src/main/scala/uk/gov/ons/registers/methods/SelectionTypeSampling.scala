package uk.gov.ons.registers.methods

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.ParamValidation
import uk.gov.ons.registers.model.SelectionTypes.{Census, PrnSampling}
import uk.gov.ons.registers.model.selectionstrata.SelectionStrata

trait SelectionTypeSampling[A] {
  def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame]
}

object SelectionTypeSampling{
  import uk.gov.ons.registers.methods.impl.SampleImpl._

//  def selectionTypeSampling[A: SelectionTypeSampling](dataFrame: DataFrame, row: SelectionStrata): Option[DataFrame] =
//    implicitly[SelectionTypeSampling[A]].sampling(dataFrame, row)
  def selectionTypeSampling[A: SelectionTypeSampling](dataFrame: DataFrame, row: SelectionStrata): Option[DataFrame] =
    SelectionTypeSampling[A].sampling(dataFrame, row)

  def apply[A](implicit process: SelectionTypeSampling[A]): SelectionTypeSampling[A] = process

  implicit val censusSample: SelectionTypeSampling[Census] =
    new SelectionTypeSampling[Census] {
      def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame] = {
        Some(stratifiedFrameDF.sample1(selectionStrata.cell_no))
      }
    }

  implicit val prnSamplingSample: SelectionTypeSampling[PrnSampling] =
    new SelectionTypeSampling[PrnSampling] {
      def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame] = {
        ParamValidation.validate(inputDF = stratifiedFrameDF, strataNumber = selectionStrata.cell_no, startingPrn = selectionStrata.prn_start,
          sampleSize = selectionStrata.no_reqd).map( sampleSize =>
          stratifiedFrameDF.sample1(selectionStrata.prn_start, sampleSize, selectionStrata.cell_no)
        )
      }
    }
}
