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
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = true))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  def castWithUnitMandatoryFields: DataFrame => DataFrame =
    (CommonFrameAndPropertiesFieldsCasting.checkUnitForMandatoryFields _).andThen(df =>
    df.withColumn(colName = cellNumber, df.col(cellNumber).cast(IntegerType)))

  def castWithStratifiedUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkStratifiedFrameForMandatoryFields

  def castWithPayeUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkPayeForMandatoryFields

  def castWithVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkVatForMandatoryFields

  def castWithGroupVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkGroupVatForMandatoryFields

  def castWithAppVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkAppVatForMandatoryFields

  def castWithCntVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkCntVatForMandatoryFields

  def castWithStdVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkStdVatForMandatoryFields

  def castWithEmploymentUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkEmploymentForMandatoryFields

  def emptyDataFrame: DataFrame = {
    val nonsensicalSchema =
      StructType(
        StructField("none", StringType, nullable = true) ::
        StructField("none", StringType, nullable = true) ::
        Nil
      )
    sparkSession.createDataFrame(rowRDD = sparkSession.sparkContext.emptyRDD[Row], schema = nonsensicalSchema)
  }

  def toNull(df: DataFrame): DataFrame = {
    val sqlCtx = df.sqlContext
    val schema = df.schema
    val rdd = df.rdd.map(
      row =>
        row.toSeq.map {
          case "" => null
          case "null" => null
          case otherwise => otherwise
        })
      .map(Row.fromSeq)

    sqlCtx.createDataFrame(rdd, schema)
  }

}
