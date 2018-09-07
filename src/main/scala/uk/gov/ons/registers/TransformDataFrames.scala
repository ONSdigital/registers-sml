package uk.gov.ons.registers

import java.io.FileNotFoundException
import java.nio.file.Path

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql._

import uk.gov.ons.api.java.methods.registers.annotation.Unused
import uk.gov.ons.registers.helpers.CSVProcessor.{DefaultFileDelimiter, readCsvFileAsDataFrame, readCsvFileAsDataset, readFileAsSQLDataContainerElseException}
import uk.gov.ons.registers.helpers.EitherSupport.fromEithers
import uk.gov.ons.registers.helpers.{CSVProcessor, TrySupport}
import uk.gov.ons.registers.model.selectionstrata.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._
import uk.gov.ons.registers.model.selectionstrata.{SelectionStrata, StratificatonPropertyFieldsCasting}

object TransformDataFrames {
  private def schemaOf[A: TypeTag]: StructType =
    ScalaReflection
      .schemaFor[A]
      .dataType
      .asInstanceOf[StructType]

  // TODO - clean failure of SelectionStrata instance
  private def asInstanceOfStratifiedProperties(rawPropertiesDf: DataFrame)
    (implicit sparkSession: SparkSession): Either[Throwable, Dataset[SelectionStrata]] = {
    import sparkSession.implicits._

    TrySupport.toEither(
      TrySupport.fold(StratificatonPropertyFieldsCasting.castRequiredPropertyFields(rawPropertiesDf))(
        err => throw err,
        castedDataFrame => Try(castedDataFrame.as[SelectionStrata])
      )
    )
  }

  def filterByCellNumber(dataFrame: DataFrame)(thisCellNumber: Int): Dataset[Row] =
    dataFrame.filter(_.getAs[Int](cellNumber) == thisCellNumber)

  // TODO - Throws error as a temporary solution until reporting is introduced and log error
  def validateAndParseInputs(propertiesDf: DataFrame, unitDf: DataFrame, validateFields: DataFrame => DataFrame)
    (implicit sparkSession: SparkSession): (DataFrame, Dataset[SelectionStrata]) = {
    val dataInputDfOrError = TrySupport.toEither(Try(validateFields(unitDf)))
    val propertiesDsOrError = asInstanceOfStratifiedProperties(propertiesDf)
    fromEithers(dataInputDfOrError, propertiesDsOrError)(
      onFailure = errs => throw new Exception(errs.map(_.getMessage).mkString(DefaultFileDelimiter)),
      onSuccess = (inputDataFrame, propertiesDataset) => inputDataFrame -> propertiesDataset)
  }

  def transformToDataFrame(arrayOfDatasets: Array[Dataset[Row]])(implicit sparkSession: SparkSession): Dataset[Row] = {
    import sparkSession.{createDataFrame, sparkContext}

    val emptyDF = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    arrayOfDatasets.fold(emptyDF)((curr, next) => curr.union(next))
  }

  @Unused(notes = "Temporarily using asInstanceOfStratifiedProperties()")
  def asInstanceOfStratifiedPropertiesTParam[T: Encoder : TypeTag](rawPropertiesDf: DataFrame)
    (implicit sparkSession: SparkSession): Either[Throwable, Dataset[T]] =
    TrySupport.toEither(
      Try(sparkSession.createDataFrame(
        rawPropertiesDf
          .withColumn(colName = prnStartPoint, col = rawPropertiesDf.col(prnStartPoint)
            .cast(DataTypes.createDecimalType(precision, scale)))
          .rdd, schemaOf[T])
        .as[T]
      )
    )

  @deprecated
  private def readInputDataAsDF(dataInputPath: Path)(implicit sparkSession: SparkSession): Either[Throwable, DataFrame] =
    readFileAsSQLDataContainerElseException[DataFrame](
      readFromFileFunc = readCsvFileAsDataFrame, filePathStr = dataInputPath)

  @deprecated
  private def readPropertiesAsDs[T : Encoder : TypeTag](propertiesPath: Path)
     (implicit sparkSession: SparkSession): Either[Throwable, Dataset[T]] =
    readFileAsSQLDataContainerElseException[Dataset[T]](
      readFromFileFunc = readCsvFileAsDataset[T], filePathStr = propertiesPath)

  @deprecated
  def validateAndConstructInputs[T : Encoder : TypeTag](properties: Path, dataFile: Path)
     (implicit sparkSession: SparkSession): (DataFrame, Dataset[T]) = {
    val dataInputDfOrError = readInputDataAsDF(properties)
    val propertiesDsOrError = readPropertiesAsDs[T](dataFile)
    fromEithers(dataInputDfOrError, propertiesDsOrError)(
      onFailure = errs => throw new Exception(errs.map(_.getMessage).mkString(DefaultFileDelimiter)),
      onSuccess = (inputDataFrame, propertiesDataset) => inputDataFrame -> propertiesDataset)
  }

  @deprecated
  def validateOutputDirectory(outputPath: Path): Unit =
    if (!outputPath.toFile.isDirectory) throw new FileNotFoundException(s"Cannot find output directory [$outputPath]")

  // Throws error as a temporary solution until reporting is introduced
  @deprecated
  def exportDfAsCsvOrError(dataFrame: DataFrame, path: Path, headerOption: Boolean = true): Unit =
    TrySupport.toEither( Try(CSVProcessor.export(dataFrame, path, headerOption)) )
      .fold(err => throw new Exception(s"Failed to export to CSV with error: ${err.getMessage}"), identity)
}
