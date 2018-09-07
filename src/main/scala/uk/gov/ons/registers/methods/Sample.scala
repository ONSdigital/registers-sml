package uk.gov.ons.registers.methods

import javax.inject.Singleton

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import uk.gov.ons.registers.TransformDataFrames.{transformToDataFrame, validateAndParseInputs}
import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting.checkStratifiedFrameForMandatoryFields
import uk.gov.ons.registers.model.SelectionTypes.Initial
import uk.gov.ons.registers.model.selectionstrata.SelectionStrata
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.selectionType

@Singleton
class Sample(implicit activeSession: SparkSession) {
  def create(stratifiedFrameDf: DataFrame, stratificationPropsDf: DataFrame): DataFrame = {
    val (stratifiedFrameDF, stratificationPropsDS) =
      validateAndParseInputs(propertiesDf = stratificationPropsDf, unitDf = stratifiedFrameDf,
        validateFields = checkStratifiedFrameForMandatoryFields)
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`
    /**
      * NOTE - the driver is solely aware of the type T in Dataset[T] and cannot be inferred by worker nodes.
      *        A transformation cannot be executed inside another transformation for another type.
      *        Collect forces the transformation to be returned to the driver allowing the proceeding step to incur
      *        as desired
      */
    val arrayOfSamples: Array[DataFrame] = stratificationPropsDS
      .filter(checkSelType(Initial.census) || checkSelType(Initial.prnSampling)).collect
      .flatMap{ selectionStrata: SelectionStrata =>
        SelectionTypeSampling.getSamplingMethod(selectionStrata).flatMap{ sampleMethod =>
          sampleMethod.sampling(stratifiedFrameDF, selectionStrata)
        }
      }
    transformToDataFrame(arrayOfDatasets = arrayOfSamples)
  }
}

object Sample {
  def sample(implicit sparkSession: SparkSession): Sample =
    new Sample
}

