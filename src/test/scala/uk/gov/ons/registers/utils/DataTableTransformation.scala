package uk.gov.ons.registers.utils

import java.io.File

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber
import uk.gov.ons.registers.support.AssertionHelpers.assertAndReturnCsvOfSampleCollection
import uk.gov.ons.registers.utils.FileProcessorHelper.lineAsListOfFields
import uk.gov.ons.stepdefs.Helpers.sparkSession

import cucumber.api.DataTable

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

  @deprecated
  private def createDataFrameOLD(aListOfLines: Seq[List[String]]): DataFrame = {
    val rows = aListOfLines.drop(HeaderIndex).map(Row.fromSeq)
    val rdd = sparkSession.sparkContext.makeRDD(rows)
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = false))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  @deprecated
  def createExpectedDataFrame(dataTable: DataTable): DataFrame = {
    val aListOfExpectedRows = dataTable.asLists(classOf[String])
    createDataFrameOLD(aListOfLines = aListOfExpectedRows.asScala.toList.map(_.asScala.toList))
  }

  @deprecated
  def createCsvOutputDataFrame: DataFrame = {
    val csvOutput = assertAndReturnCsvOfSampleCollection(outputPath = new File("").toPath)
    val csvFileAsLists = lineAsListOfFields(file = csvOutput)
    createDataFrameOLD(aListOfLines = csvFileAsLists)
  }
}
