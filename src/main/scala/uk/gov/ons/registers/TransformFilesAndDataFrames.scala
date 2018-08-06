package uk.gov.ons.registers

import java.nio.file.Path

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql._

import uk.gov.ons.registers.SparkSessionManager.sparkSession.{createDataFrame, sparkContext}
import uk.gov.ons.registers.helpers.CSVProcessor._
import uk.gov.ons.registers.helpers.EitherSupport.fromEithers
import uk.gov.ons.registers.helpers.TrySupport

object TransformFilesAndDataFrames {

  // TODO - Return report and log error
  def validateAndConstructInputs[T : Encoder : TypeTag](properties: Path, dataFile: Path)
                                                       (implicit sparkSession: SparkSession): (DataFrame, Dataset[T]) = {
    val dataInputDfOrError = readInputDataAsDF(properties)
    val propertiesDsOrError = readPropertiesAsDs[T](dataFile)
    fromEithers(dataInputDfOrError, propertiesDsOrError)(
      onFailure = errs => throw new Exception(errs.map(_.getMessage).mkString(",")),
      onSuccess = (inputDataFrame, propertiesDataset) => inputDataFrame -> propertiesDataset)
  }

  private def readInputDataAsDF(dataInputPath: Path)(implicit sparkSession: SparkSession): Either[Throwable, DataFrame] =
    readFileAsSQLDataContainerElseException[DataFrame](
      readFromFileFunc = readCsvFileAsDataFrame, filePathStr = dataInputPath)

  private def readPropertiesAsDs[T : Encoder : TypeTag](propertiesPath: Path)
   (implicit sparkSession: SparkSession): Either[Throwable, Dataset[T]] =
    readFileAsSQLDataContainerElseException[Dataset[T]](
      readFromFileFunc = readCsvFileAsDataset[T], filePathStr = propertiesPath)

  def tranformToDataFrame(arrayOfDatasets: Array[Dataset[Row]]): DataFrame = {
    val emptyDF = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    arrayOfDatasets.foldLeft(emptyDF)((curr, next) => curr.union(next))
  }

  @deprecated("Migarate to exportDfAsCsvOrError()")
  def exportDatasetAsCSV(arrayOfDatasets: Array[Dataset[Row]], outputPath: Path): DataFrame = {
    val outputDF = tranformToDataFrame(arrayOfDatasets)
    exportDfAsCsvOrError(dataFrame = outputDF, path = outputPath)
    outputDF
  }

  // Throws error as a temporary solution until reporting is introduced
  def exportDfAsCsvOrError(dataFrame: DataFrame, path: Path, headerOption: Boolean = true): Unit = {
    TrySupport.toEither(
      Try(export(dataFrame, path, headerOption))
    ).fold(err => throw new Exception(s"Failed to export to CSV with error: ${err.getMessage}"), identity)
  }
}