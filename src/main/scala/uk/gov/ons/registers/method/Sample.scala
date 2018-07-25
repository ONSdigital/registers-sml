package uk.gov.ons.registers.method

import org.apache.spark.sql._

import uk.gov.ons.registers.helpers.CSVProcessor.FilePath
import uk.gov.ons.registers.model.SelectionTypes
import uk.gov.ons.registers.model.SelectionTypes.{census, prnSampling}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.selectionType
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord
import uk.gov.ons.registers.{SparkSessionManager, TransformFiles}

class Sample(inputPath: FilePath) {

  import uk.gov.ons.registers.method.impl.SampleImpl._
  // TODO - USE SparkSessionManager.withSpark
  // TODO - ADD logggers/ logging
  def create(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {
    implicit val activeSession: SparkSession = SparkSessionManager.sparkSession

    val inputDataDF =  TransformFiles.readInputDataAsDF(inputPath)
    val stratificationPropsDS = TransformFiles.readStratificationPropsAsDS(stratificationPropsPath)
    val inputDfSize = inputDataDF.count.toInt
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - Check Join
    val arrayOfSamples: Array[DataFrame] = stratificationPropsDS
      .filter(checkSelType(census) || checkSelType(prnSampling)).rdd.collect
      .map{ row: StratificationPropertiesRecord =>
          if (row.seltype == SelectionTypes.prnSampling)
              inputDataDF.sample1(row.prn_start, row.no_reqd, row.cell_no)

          else inputDataDF.sample1(row.cell_no)
      }

    val sampleDF = TransformFiles.exportDatasetAsCSV(arrayOfDatasets = arrayOfSamples, outputPath = outputPath)
    sampleDF
  }
}

object Sample {
  def sample(inputPath: FilePath): Sample = new Sample(inputPath)
}

