package uk.gov.ons.registers

import java.nio.file.Path

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql._

import uk.gov.ons.registers.SparkSessionManager.sparkSession.{createDataFrame, sparkContext}
import uk.gov.ons.registers.helpers.CSVProcessor._
import uk.gov.ons.registers.helpers.EitherSupport.fromEithers

object TransformFiles {

  def validateAndConstructInputs[T : Encoder : TypeTag](properties: Path, dataFile: Path)
                                                       (implicit sparkSession: SparkSession): (DataFrame, Dataset[T]) = {
    val dataInputDfOrError = TransformFiles.readInputDataAsDF(properties)
    val propertiesDsOrError = TransformFiles.readPropertiesAsDs[T](dataFile)
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

  def exportDatasetAsCSV(arrayOfDatasets: Array[Dataset[Row]], outputPath: Path): DataFrame = {
    val emptyDF = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    val outputDF = arrayOfDatasets.foldLeft(emptyDF)((curr, next) => curr.union(next))
    export(dataFrame = outputDF, path = outputPath)
    outputDF
  }

}