package uk.gov.ons.registers.methods

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.ParamValidation
import uk.gov.ons.registers.methods.impl.SampleImpl._
import uk.gov.ons.registers.model.SelectionTypes.Initial
import uk.gov.ons.registers.model.selectionstrata.{SelectionStrata, StratificationPropertiesFields}

trait SelectionTypeSampling {
  def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame]
}

object SelectionTypeSampling {
  private object PrnSampling extends SelectionTypeSampling {
    override def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame] = {
      ParamValidation.validate(inputDF = stratifiedFrameDF, strataNumber = selectionStrata.cell_no,
        startingPrn = selectionStrata.prn_start, sampleSize = selectionStrata.no_reqd).map( sampleSize =>
        stratifiedFrameDF.sample1(selectionStrata.prn_start, sampleSize, selectionStrata.cell_no))
    }
  }

  private object CensusSampling extends SelectionTypeSampling {
    override def sampling(stratifiedFrameDF: DataFrame, selectionStrata: SelectionStrata): Option[DataFrame] = {
      Some(stratifiedFrameDF.sample1(selectionStrata.cell_no))
    }
  }

  def getSamplingMethod(selectionStrata: SelectionStrata): Option[SelectionTypeSampling] = {
    selectionStrata.seltype match {
      case Initial.prnSampling => Some(PrnSampling)
      case Initial.census => Some(CensusSampling)
      case _ =>
        println(s"Failed to parse field [${StratificationPropertiesFields.selectionType}] in strata [${selectionStrata.cell_no}]")
        None
    }
  }
}
