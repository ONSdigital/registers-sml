package uk.gov.ons.registers.utils

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber
import uk.gov.ons.stepdefs.Helpers.sparkSession

object DataTableTransformation {
  private val HeaderIndex = 1

  type RawDataTableList = java.util.List[java.util.List[String]]

  def createDataFrame: RawDataTableList => DataFrame =
    (fromDataTableToLists _).andThen(toDataFrame)

  private def fromDataTableToLists(rawJavaList: RawDataTableList): List[List[String]] =
    rawJavaList.asScala.toList.map(_.asScala.toList)

  private def toDataFrame(aListOfLines: List[List[String]]): DataFrame = {
    val rows = aListOfLines.drop(HeaderIndex).map(Row.fromSeq)
    val rdd = sparkSession.sparkContext.makeRDD(rows)
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = false))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  def castWithUnitMandatoryFields: DataFrame => DataFrame =
    (CommonFrameAndPropertiesFieldsCasting.checkUnitForMandatoryFields _).andThen(df =>
    df.withColumn(colName = cellNumber, df.col(cellNumber).cast(IntegerType)))

  def castWithStratifiedUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkStratifiedFrameForMandatoryFields

  def emptyDataFrame: DataFrame = {
    val nonsensicalSchema =
      StructType(
        StructField("none", StringType, nullable = true) ::
        StructField("none", StringType, nullable = true) ::
        Nil
      )
    sparkSession.createDataFrame(rowRDD = sparkSession.sparkContext.emptyRDD[Row], schema = nonsensicalSchema)
  }
}
