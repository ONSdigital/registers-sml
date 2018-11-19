package uk.gov.ons.registers.model.selectionstrata

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.util.Try
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
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


  def mapStartificationProperties(rawPropertiesDf: DataFrame)(implicit spark:SparkSession): DataFrame = {

    val schema = new StructType().add(StructField(inqueryCode, IntegerType, true))
      .add(StructField(cellNumber, IntegerType, true))
      .add(StructField(lowerClassSIC07, IntegerType, true))
      .add(StructField(upperClassSIC07, IntegerType, true))
      .add(StructField(lowerSizeBounds, LongType, true))
      .add(StructField(upperSizeBounds, LongType, true))
      .add(StructField(prnStartPoint, DataTypes.createDecimalType(precision, scale), true))
      .add(StructField(sampleSize, IntegerType, true))

    val df: RDD[Row] = rawPropertiesDf.rdd.map(row =>

      new GenericRowWithSchema(Array(
        row.getAs[String](inqueryCode).toInt,
        row.getAs[String](cellNumber).toInt,
        row.getAs[String](lowerClassSIC07).toInt,
        row.getAs[String](upperClassSIC07).toInt,
        row.getAs[String](lowerSizeBounds).toLong,
        row.getAs[String](upperSizeBounds).toLong,
        BigDecimal(row.getAs[String](prnStartPoint)).setScale(10),
        row.getAs[String](sampleSize).toInt
      ), schema))
    spark.createDataFrame(df, schema)
  }
}
