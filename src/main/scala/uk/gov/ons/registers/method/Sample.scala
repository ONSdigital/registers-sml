package uk.gov.ons.registers.method

import org.apache.spark.sql.{SparkSession, DataFrame, Column}

import uk.gov.ons.registers.EitherSupport.fromEithers
import uk.gov.ons.registers.helpers.CSVProcessor.FilePath
import uk.gov.ons.registers.model.SelectionTypes
import uk.gov.ons.registers.model.SelectionTypes.{census, prnSampling}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord
import uk.gov.ons.registers.{ParamValidation, SparkSessionManager, TransformFiles}

class Sample(stratifiedFramePath: FilePath) {

  import uk.gov.ons.registers.method.impl.SampleImpl._
  // TODO - USE SparkSessionManager.withSpark
  // TODO - ADD logggers/ logging

  def create(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {
    implicit val activeSession: SparkSession = SparkSessionManager.sparkSession

    val stratifiedFrameDfOrError = TransformFiles.readInputDataAsDF(stratifiedFramePath)
    val stratificationPropsDsOrError = TransformFiles.readStratificationPropsAsDs(stratificationPropsPath)

    // NOTE - In future we return a Report [C] not throw an Exception
    val (stratifiedFrameDF, stratificationPropsDS) =
      fromEithers(stratifiedFrameDfOrError, stratificationPropsDsOrError)(
        onFailure = errs => throw new Exception(s"${errs.map(_.getMessage)}"),
        onSuccess = (dataFrame, stratificationPropsDataset) => dataFrame -> stratificationPropsDataset)

    val inputDfSize = stratifiedFrameDF.count.toInt
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - Check Join
    val arrayOfSamples = stratificationPropsDS
      .filter(checkSelType(census) || checkSelType(prnSampling)).rdd.collect
      .flatMap{ row: StratificationPropertiesRecord =>
          if (row.seltype == SelectionTypes.prnSampling)
            ParamValidation.validate(maxSize = inputDfSize, strataNumber = row.cell_no, startingPrn = row.prn_start,
              sampleSize = row.no_reqd).map( sampleSize =>
                stratifiedFrameDF.sample1(row.prn_start, sampleSize, row.cell_no)
              )
          else Some(stratifiedFrameDF.sample1(row.cell_no))
      }

    val sampleDF = TransformFiles.exportDatasetAsCSV(arrayOfDatasets = arrayOfSamples, outputPath = outputPath)
    sampleDF
  }
}

object Sample {
  def sample(inputPath: FilePath): Sample = new Sample(inputPath)
}

