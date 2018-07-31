package uk.gov.ons.registers

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, _}

import uk.gov.ons.registers.SparkSessionManager.sparkSession.{createDataFrame, sparkContext}
import uk.gov.ons.registers.helpers.CSVProcessor._
import uk.gov.ons.registers.helpers.EitherSupport.fromEithers
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord

object TransformFiles {
  import SparkSessionManager.sparkSession.implicits._

  def validateAndConstructInputs[T : Encoder : TypeTag](properties: FilePath, dataFile: FilePath)
   (implicit sparkSession: SparkSession): (DataFrame, Dataset[T]) = {
    val dataInputDfOrError = TransformFiles.readInputDataAsDF(properties)
    val propertiesDsOrError = TransformFiles.readPropertiesAsDs[T](dataFile)

    fromEithers(dataInputDfOrError, propertiesDsOrError)(
      onFailure = errs => throw new Exception(errs.map(_.getMessage).mkString(",")),
      onSuccess = (dataFrame, propertiesDataset) => dataFrame -> propertiesDataset)
  }

  @deprecated("Migrate to readInputDataAsDF")
  def readInputDataAsDfOLD(inputPath: FilePath)(implicit sparkSession: SparkSession): DataFrame =
    readFileAsSQLDataContainerElseExceptionOLD[DataFrame](
    readFromFileFunc = readCsvFileAsDataFrame, filePathStr = inputPath)

  private def readInputDataAsDF(dataInputPath: FilePath)(implicit sparkSession: SparkSession): Either[Throwable, DataFrame] =
    readFileAsSQLDataContainerElseException[DataFrame](
      readFromFileFunc = readCsvFileAsDataFrame, filePathStr = dataInputPath)

  @deprecated("Migrate to readStratificationPropsAsDs")
  def readStratificationPropsAsDsOLD(stratificationPropsPath: FilePath)(implicit sparkSession: SparkSession): Dataset[StratificationPropertiesRecord] =
    readFileAsSQLDataContainerElseExceptionOLD[Dataset[StratificationPropertiesRecord]](
    readFromFileFunc = readCsvFileAsDataset[StratificationPropertiesRecord], filePathStr = stratificationPropsPath)

  private def readPropertiesAsDs[T: Encoder : TypeTag](propertiesPath: FilePath)
    (implicit sparkSession: SparkSession): Either[Throwable, Dataset[T]] =
    readFileAsSQLDataContainerElseException[Dataset[T]](
      readFromFileFunc = readCsvFileAsDataset[T], filePathStr = propertiesPath)

  def exportDatasetAsCSV(arrayOfDatasets: Array[Dataset[Row]], outputPath: FilePath): DataFrame = {
    val empty = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    val outputDF = arrayOfDatasets.foldLeft(empty)((curr, next) => curr.union(next))
    export(dataFrame = outputDF, path = outputPath)
    outputDF
  }

}
