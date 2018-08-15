package uk.gov.ons.registers.utils

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import uk.gov.ons.registers.stepdefs.outputPath
import uk.gov.ons.registers.support.AssertionHelpers.assertAndReturnCsvOfSampleCollection
import uk.gov.ons.registers.utils.FileProcessorHelper.lineAsListOfFields
import uk.gov.ons.stepdefs.Helpers.sparkSession

import cucumber.api.DataTable

object DataFrameTransformation {
  private val HeaderIndex = 1

  // TODO - look into cucumber transformers
  private def createDataFrame(aListOfLines: Seq[List[String]]): DataFrame = {
    val rows = aListOfLines.drop(HeaderIndex).map(Row.fromSeq(_))
    val rdd = sparkSession.sparkContext.makeRDD(rows)
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = false))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  def createExpectedDataFrame(dataTable: DataTable): DataFrame = {
    val aListOfExpectedRows = dataTable.asLists(classOf[String])
    createDataFrame(aListOfLines = aListOfExpectedRows.asScala.toList.map(_.asScala.toList))
  }

  def createCsvOutputDataFrame: DataFrame = {
    val csvOutput = assertAndReturnCsvOfSampleCollection(outputPath = outputPath)
    val csvFileAsLists = lineAsListOfFields(file = csvOutput)
    createDataFrame(aListOfLines = csvFileAsLists)
  }
}
