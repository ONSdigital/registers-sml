package uk.gov.ons.registers

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{DataTypes, StructType}
import uk.gov.ons.api.java.methods.registers.annotation.Unused
import uk.gov.ons.registers.helpers.EitherSupport.fromEithers
import uk.gov.ons.registers.helpers.TrySupport
import uk.gov.ons.registers.model.selectionstrata.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._
import uk.gov.ons.registers.model.selectionstrata.{SelectionStrata, StratificatonPropertyFieldsCasting}

object TransformDataFrames {
  val DefaultFileDelimiter = ","

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

  def validateAndParseInputsStrata(propertiesDf: DataFrame, unitDf: DataFrame, bounds: String, validateFields: (DataFrame, String) => DataFrame)
                                  (implicit sparkSession: SparkSession): (DataFrame, Dataset[SelectionStrata]) = {
    val dataInputDfOrError = TrySupport.toEither(Try(validateFields(unitDf, bounds)))
    val propertiesDsOrError = asInstanceOfStratifiedProperties(propertiesDf)
    fromEithers(dataInputDfOrError, propertiesDsOrError)(
      onFailure = errs => throw new Exception(errs.map(_.getMessage).mkString(DefaultFileDelimiter)),
      onSuccess = (inputDataFrame, propertiesDataset) => inputDataFrame -> propertiesDataset)
  }

  def fromArrayDataFrame(arrayOfDatasets: Array[Dataset[Row]])(implicit sparkSession: SparkSession): Dataset[Row] = {
    import sparkSession.{createDataFrame, sparkContext}

    val emptyDF = createDataFrame(sparkContext.emptyRDD[Row], arrayOfDatasets.head.schema)
    arrayOfDatasets.fold(emptyDF)((curr, next) => curr.union(next))
  }

  def createCellNumberCounts(dataFrame: DataFrame)(implicit sparkSession: SparkSession): List[CellNumberCount] = {
    import sparkSession.implicits._
    dataFrame.groupBy(cellNumber).count().as[CellNumberCount].collect().toList
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
}

case class CellNumberCount(cell_no:Int, count:Long)
