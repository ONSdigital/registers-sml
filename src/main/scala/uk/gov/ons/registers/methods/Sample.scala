package uk.gov.ons.registers.methods

import java.nio.file.Path

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import uk.gov.ons.registers.TransformFilesAndDataFrames.exportDfAsCsvOrError
import uk.gov.ons.registers.model.SelectionTypes
import uk.gov.ons.registers.model.SelectionTypes.{census, prnSampling}
import uk.gov.ons.registers.model.stratification.Strata
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.{ParamValidation, SparkSessionManager, TransformFilesAndDataFrames}

class Sample(stratifiedFramePath: Path)(implicit activeSession: SparkSession) {

  import uk.gov.ons.registers.methods.impl.SampleImpl._

  import activeSession.implicits._

  // TODO - ADD logggers/ logging
  def create(stratificationPropsPath: Path, outputPath: Path): DataFrame = {

    val (stratifiedFrameDF, stratificationPropsDS) =
      TransformFilesAndDataFrames.validateAndConstructInputs[Strata](
        properties = stratifiedFramePath, dataFile = stratificationPropsPath)
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - Check Join || make inputDF distributed and pass props
    val arrayOfSamples = stratificationPropsDS
      .filter(checkSelType(census) || checkSelType(prnSampling)).rdd.collect
      .flatMap{ row: Strata =>
        if (row.seltype == SelectionTypes.prnSampling)
        // TODO - type classes for prn-sampling + validation there and another with census with no validation
        // read in row.seltype as case object to figure out which type of op it should be - getting right instance
          ParamValidation.validate(inputDF = stratifiedFrameDF, strataNumber = row.cell_no, startingPrn = row.prn_start,
            sampleSize = row.no_reqd).map( sampleSize =>
            stratifiedFrameDF.sample2(row.prn_start, sampleSize, row.cell_no)
          )
        else Some(stratifiedFrameDF.sample2(row.cell_no))
      }

    val sampleStratasDF = TransformFilesAndDataFrames.tranformToDataFrame(arrayOfDatasets = arrayOfSamples)
    exportDfAsCsvOrError(dataFrame = sampleStratasDF, path = outputPath)
    SparkSessionManager.stopSession()
    sampleStratasDF
  }
}

object Sample {
  def sample(inputPath: Path)(implicit sparkSession: SparkSession): Sample =
    new Sample(inputPath)(sparkSession)
}

