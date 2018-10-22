package uk.gov.ons.registers.model.selectionstrata

import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType}

import uk.gov.ons.registers.model.selectionstrata.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields._


object StratificatonPropertyFieldsCasting {
  def castRequiredPropertyFields(rawPropertiesDf: DataFrame): Try[DataFrame] =
    Try(rawPropertiesDf
      .withColumn(colName = inqueryCode, rawPropertiesDf.col(inqueryCode).cast(IntegerType))
      .withColumn(colName = cellNumber, rawPropertiesDf.col(cellNumber).cast(IntegerType))
      .withColumn(colName = lowerClassSIC07, rawPropertiesDf.col(lowerClassSIC07).cast(IntegerType))
      .withColumn(colName = upperClassSIC07, rawPropertiesDf.col(upperClassSIC07).cast(IntegerType))
      .withColumn(colName = lowerSizeBounds, rawPropertiesDf.col(lowerSizeBounds).cast(LongType))
      .withColumn(colName = upperSizeBounds, rawPropertiesDf.col(upperSizeBounds).cast(LongType))
      .withColumn(colName = prnStartPoint, col = rawPropertiesDf.col(prnStartPoint)
        .cast(DataTypes.createDecimalType(precision, scale)))
      .withColumn(colName = sampleSize, rawPropertiesDf.col(sampleSize).cast(IntegerType)))
}
