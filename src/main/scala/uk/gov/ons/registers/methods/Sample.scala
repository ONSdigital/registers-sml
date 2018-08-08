package uk.gov.ons.registers.methods

import java.nio.file.Path

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import uk.gov.ons.registers.TransformFilesAndDataFrames.exportDfAsCsvOrError
import uk.gov.ons.registers.model.SelectionTypes.Initial
import uk.gov.ons.registers.model.stratification.SelectionStrata
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.{ParamValidation, SparkSessionManager, TransformFilesAndDataFrames}

class Sample(stratifiedFramePath: Path)(implicit activeSession: SparkSession) {

  import uk.gov.ons.registers.methods.impl.SampleImpl._

  import activeSession.implicits._

  // TODO - ADD loggers/ logging
  def create(stratificationPropsPath: Path, outputPath: Path): DataFrame = {

    val (stratifiedFrameDF, stratificationPropsDS) =
      TransformFilesAndDataFrames.validateAndConstructInputs[SelectionStrata](
        properties = stratifiedFramePath, dataFile = stratificationPropsPath)
    TransformFilesAndDataFrames.validateOutputDirectory(outputPath)
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - Check Join || make inputDF distributed and pass props
    val arrayOfSamples = stratificationPropsDS
      .filter(checkSelType(Initial.census) || checkSelType(Initial.prnSampling)).rdd.collect
      .flatMap{ selectionStrata: SelectionStrata =>
        if (selectionStrata.seltype == Initial.prnSampling)
        // TODO - type classes for prn-sampling + validation there and another with census with no validation
        // read in selectionStrata.seltype as case object to figure out which type of op it should be - getting right instance
          ParamValidation.validate(inputDF = stratifiedFrameDF, strataNumber = selectionStrata.cell_no, startingPrn = selectionStrata.prn_start,
            sampleSize = selectionStrata.no_reqd).map( sampleSize =>
            stratifiedFrameDF.sample2(selectionStrata.prn_start, sampleSize, selectionStrata.cell_no)
          )
        else Some(stratifiedFrameDF.sample2(selectionStrata.cell_no))
      }

    val sampleStratasDF = TransformFilesAndDataFrames.transformToDataFrame(arrayOfDatasets = arrayOfSamples)
    exportDfAsCsvOrError(dataFrame = sampleStratasDF, path = outputPath)
    SparkSessionManager.stopSession()
    sampleStratasDF
  }
}

object Sample {
  def sample(inputPath: Path)(implicit sparkSession: SparkSession): Sample =
    new Sample(inputPath)(sparkSession)
}

