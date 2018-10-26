package uk.gov.ons.registers.methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.{ParamValidation, Patch, TransformDataFrames}
import uk.gov.ons.registers.methods.impl.SampleImpl._
import uk.gov.ons.registers.model.SelectionTypes.Initial
import uk.gov.ons.registers.model.selectionstrata.{SelectionStrata, StratificationPropertiesFields}

sealed trait SelectionTypeSampling {
  def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata)(implicit activeSession: SparkSession): Option[DataFrame]
}

object SelectionTypeSampling {
  private object PrnSampling extends SelectionTypeSampling {
    override def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata)(implicit activeSession: SparkSession): Option[DataFrame] =
      ParamValidation.validate(counts =  TransformDataFrames.createCellNumberCounts(stratifiedFrameDF), strataNumber = selectionStrata.cell_no,
        startingPrn = selectionStrata.prn_start, sampleSize = selectionStrata.no_reqd).map( sampleSize =>
        stratifiedFrameDF.sample1(selectionStrata.prn_start, sampleSize, selectionStrata.cell_no))
  }

  private object CensusSampling extends SelectionTypeSampling {
    override def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata)(implicit activeSession: SparkSession): Option[DataFrame] =
      Some(stratifiedFrameDF.sample1(selectionStrata.cell_no))
  }

  private[methods] def getMethod(selectionStrata: SelectionStrata): Option[SelectionTypeSampling] =
    selectionStrata.seltype match {
      case Initial.prnSampling => Some(PrnSampling)
      case Initial.census => Some(CensusSampling)
      case Initial.universal => None
      case e: String =>
        Patch.log(level = "warn", msg = s"Failed to parse [$e] in field " +
          s"[${StratificationPropertiesFields.selectionType}] in strata [${selectionStrata.cell_no}]")
        None
    }
}
