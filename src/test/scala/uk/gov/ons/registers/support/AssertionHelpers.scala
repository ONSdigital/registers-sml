package uk.gov.ons.registers.support

import java.io.File
import java.nio.file.Path

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.helpers.CSVProcessor.CSV
import uk.gov.ons.registers.stepdefs.outputDataDF
import uk.gov.ons.registers.support.DataFrameTransformation.{createCsvOutputDataFrame, createExpectedDataFrame}

import cucumber.api.DataTable

object AssertionHelpers {
  def assertAndReturnCsvOfSampleCollection(outputPath: Path): File = {
    val sampleOutputDir = outputPath.toFile
    assert(sampleOutputDir.exists && sampleOutputDir.isDirectory, message = s"output path [$outputPath] does not exist and/ or is not a directory")
    val listOfCsvOutputFiles = sampleOutputDir.listFiles.filter(_.getName.endsWith(s".$CSV"))
    assert(listOfCsvOutputFiles.nonEmpty, message = s"found no files with extension [.$CSV] in [$outputPath] directory")
    listOfCsvOutputFiles.head
  }

  def assertDataFrameEquality(expected: DataTable): DataFrame = {
    val expectedOutputDF = createExpectedDataFrame(expected)
    assert(outputDataDF.collect sameElements expectedOutputDF.collect)
    val csvFileOutputDF = createCsvOutputDataFrame
    assert(csvFileOutputDF.collect sameElements expectedOutputDF.collect)
    expectedOutputDF
  }

  def aFailureIsGeneratedBy[T](expression: => T): Boolean =
    try {
      expression
      false
    } catch {
      case _: Throwable => true
    }

  def displayData(expectedDF: DataFrame, printLabel: String): Unit = {
    println("Compare Rows")
    println(s"Expected $printLabel Output")
    expectedDF.show()
    println(s"Scala $printLabel output")
    outputDataDF.show()
  }
}
