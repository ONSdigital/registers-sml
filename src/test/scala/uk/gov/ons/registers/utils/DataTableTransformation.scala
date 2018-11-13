package uk.gov.ons.registers.utils

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting
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

  def castWithUnitMandatoryFields(implicit sparkSession: SparkSession): (DataFrame, String) => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkUnitForMandatoryFields

  def castWithStratifiedUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkStratifiedFrameForMandatoryFields

  def castWithPayeUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkPayeforMandatoryFields

  def castWithVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkVatforMandatoryFields

  def castWithGroupVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkGroupVatforMandatoryFields

  def castWithAppVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkAppVatforMandatoryFields

  def castWithCntVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkCntVatforMandatoryFields

  def castWithStdVatUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkStdVatforMandatoryFields

  def castWithEmploymentUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkEmploymentforMandatoryFields

  def castWithImputedUnitMandatoryFields: DataFrame => DataFrame =
    CommonFrameAndPropertiesFieldsCasting.checkImputedforMandatoryFields

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

  def nullToNull(df: DataFrame): DataFrame = {
    val sqlCtx = df.sqlContext
    val schema = df.schema
    val rdd = df.rdd.map(
      row =>
        row.toSeq.map {
          case "null" => null
          case otherwise => otherwise
        })
      .map(Row.fromSeq)
    sqlCtx.createDataFrame(rdd, schema)
  }
}
