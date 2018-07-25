package uk.gov.ons.registers

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import uk.gov.ons.registers.helpers.CSVProcessor._
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord
import uk.gov.ons.registers.SparkSessionManager.sparkSession.{createDataFrame, sparkContext}

object TransformFiles {
  import SparkSessionManager.sparkSession.implicits._

  def readInputDataAsDF(inputPath: FilePath)(implicit sparkSession: SparkSession): DataFrame =
    readFileAsSQLDataContainerElseException[DataFrame](
    readFromFileFunc = readCsvFileAsDataFrame, filePathStr = inputPath)

  def readStratificationPropsAsDS(stratificationPropsPath: FilePath)(implicit sparkSession: SparkSession): Dataset[StratificationPropertiesRecord] =
    readFileAsSQLDataContainerElseException[Dataset[StratificationPropertiesRecord]](
    readFromFileFunc = readCsvFileAsDataset[StratificationPropertiesRecord], filePathStr = stratificationPropsPath)

  def exportDatasetAsCSV(arrayOfDatasets: Array[Dataset[Row]], outputPath: FilePath): DataFrame = {
    val empty = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    val outputDF = arrayOfDatasets.foldLeft(empty)((curr, next) => curr.union(next))
    export(dataFrame = outputDF, path = outputPath)
    outputDF
  }

}
