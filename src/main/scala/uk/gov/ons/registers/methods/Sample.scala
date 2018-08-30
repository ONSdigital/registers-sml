package uk.gov.ons.registers.methods

import javax.inject.Singleton

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import uk.gov.ons.registers.TransformDataFrames.{transformToDataFrame, validateAndParseInputs}
import uk.gov.ons.registers.model.SelectionTypes.Initial
import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting.checkStratifiedFrameForMandatoryFields
import uk.gov.ons.registers.model.selectionstrata.SelectionStrata
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.{ParamValidation, SparkSessionManager}

@Singleton
class Sample(implicit activeSession: SparkSession) {
  import uk.gov.ons.registers.methods.impl.SampleImpl._

  def create(stratifiedFrameDf: DataFrame, stratificationPropsDf: DataFrame): DataFrame = {
    SparkSessionManager.terminateSession{
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
      val arrayOfSamples = stratificationPropsDS
        .filter(checkSelType(Initial.census) || checkSelType(Initial.prnSampling)).collect
        .flatMap{ selectionStrata: SelectionStrata =>

//          val selectionType = TrySupport.fold(SelectionTypes.fromString(selectionStrata.seltype))(
//            ex => throw ex, identity)
//          SelectionTypeSampling.selectionTypeSampling[](stratifiedFrameDF, selectionStrata)

          if (selectionStrata.seltype == Initial.prnSampling)
            ParamValidation.validate(inputDF = stratifiedFrameDF, strataNumber = selectionStrata.cell_no,
              startingPrn = selectionStrata.prn_start, sampleSize = selectionStrata.no_reqd).map( sampleSize =>
              stratifiedFrameDF.sample1(selectionStrata.prn_start, sampleSize, selectionStrata.cell_no))
          else Some(stratifiedFrameDF.sample1(selectionStrata.cell_no))
        }
      transformToDataFrame(arrayOfDatasets = arrayOfSamples)
    }
  }
}

object Sample {
  def sample(implicit sparkSession: SparkSession): Sample =
    new Sample
}

