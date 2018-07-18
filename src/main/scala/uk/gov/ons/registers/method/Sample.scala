package uk.gov.ons.registers.method

import org.apache.spark.sql._

import uk.gov.ons.registers.helpers.CSVProcessor._
import uk.gov.ons.registers.helpers.SparkSessionManager
import uk.gov.ons.registers.helpers.SparkSessionManager.sparkSession.{createDataFrame, sparkContext}
import uk.gov.ons.registers.model.SelectionTypes.{census, prnSampling}
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields._
import uk.gov.ons.registers.model.SelectionTypes
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord

class Sample(inputPath: FilePath) {

//  implicit val activeSession: SparkSession = SparkSessionManager.sparkSession

  import SparkSessionManager.sparkSession.implicits._
  import uk.gov.ons.registers.method.impl.SampleImpl._

  // TODO - CLEAN
  // TODO - USE SparkSessionManager.withSpark
  // TODO - ADD logggers/ logging
  def create(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {
    implicit val activeSession: SparkSession = SparkSessionManager.sparkSession

    val inputDataDF = readFileAsSQLDataContainerElseException[DataFrame](
      readFromFileFunc = readCsvFileAsDataFrame, filePathStr = inputPath)
    val stratificationPropsDS = readFileAsSQLDataContainerElseException[Dataset[StratificationPropertiesRecord]](
      readFromFileFunc = readCsvFileAsDataset[StratificationPropertiesRecord], filePathStr = stratificationPropsPath)
    def checkSelType(`type`: String): Column = stratificationPropsDS(selectionType) === `type`

    // TODO - map over with DataFrame [WARN]
    // TODO - RDD approach, remove collect - distribute function enclosed in map
    val arrayOfSamples = stratificationPropsDS
      .filter(checkSelType(census) || checkSelType(prnSampling)).rdd.collect
      .map{ row: StratificationPropertiesRecord =>
        if (row.seltype == SelectionTypes.prnSampling) {
          println(s"row.prn_start ==>>> ${row.prn_start}");
          inputDataDF.sample1(row.prn_start, row.no_reqd, row.cell_no)
        } else inputDataDF.sample1(row.cell_no)
    }

//    val empty = createDataFrame(sparkContext.emptyRDD[Row], arrayOfSamples.first().schema)
//    val sampleDF = arrayOfSamples.fold(empty)((curr, next) => curr.union(next))
    val empty = createDataFrame(sparkContext.emptyRDD[Row], arrayOfSamples.head.schema)
    val sampleDF = arrayOfSamples.foldLeft(empty)((curr, next) => curr.union(next))
    export(dataFrame = sampleDF, path = outputPath)
    sampleDF
  }
}

object Sample {
  def sample(inputPath: FilePath): Sample = new Sample(inputPath)
}
