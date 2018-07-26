package uk.gov.ons.registers.method


import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import uk.gov.ons.registers.helpers.CSVProcessor.FilePath
import uk.gov.ons.registers.model.SelectionTypes
import uk.gov.ons.registers.model.SelectionTypes.{census, prnSampling}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord
import uk.gov.ons.registers.{ParamValidation, TransformFiles}

class Sample(stratifiedFramePath: FilePath)(implicit activeSession: SparkSession) {

  import uk.gov.ons.registers.method.impl.SampleImpl._

  import activeSession.implicits._
  // TODO - USE SparkSessionManager.withSpark - implicit
  // TODO - ADD logggers/ logging

  def create(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {

    val (stratifiedFrameDF, stratificationPropsDS) =
      TransformFiles.validateAndConstructInputs[StratificationPropertiesRecord](
        properties = stratifiedFramePath, dataFile = stratificationPropsPath)

    val inputDfSize = stratifiedFrameDF.count.toInt
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - Check Join
    val arrayOfSamples = stratificationPropsDS
      .filter(checkSelType(census) || checkSelType(prnSampling)).rdd.collect
      .flatMap{ row: StratificationPropertiesRecord =>
          if (row.seltype == SelectionTypes.prnSampling)
            // TODO - type classes for prn-smapling + validation there and another with census with no validation
            // read in row.seltype as case object to figure out which type of op it should be - getting right instance
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
  def sample(inputPath: FilePath)(implicit sparkSession: SparkSession): Sample =
    new Sample(inputPath)(sparkSession)
}

