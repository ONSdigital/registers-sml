package uk.gov.ons.registers.support

import java.io.File
import java.nio.file.Path

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.helpers.CSVProcessor.CSV
import uk.gov.ons.registers.stepdefs.outputDataDF
import uk.gov.ons.registers.support.DataFrameTransformation.{createCsvOutputDataFrame, createExpectedDataFrame}

import cucumber.api.DataTable
//import org.junit.Assert._

object AssertionHelpers {
  // Assert if CSV file is saved - distributed, and thus cannot use fixed naming match
  def assertAndReturnCsvOfSampleCollection(outputPath: Path): File = {
    val sampleOutputDir = outputPath.toFile
    assert(sampleOutputDir.exists && sampleOutputDir.isDirectory, message = s"output path [$outputPath] does not exist and/ or is not a directory")
    val listOfCsvOutputFiles = sampleOutputDir.listFiles.filter(_.getName.endsWith(s".$CSV"))
    assert(listOfCsvOutputFiles.nonEmpty, message = s"found no files with extension [.$CSV] in [$outputPath] directory")
    listOfCsvOutputFiles.head
  }

  def assertDataFrameEquality(expected: DataTable, printLabel: String): Unit = {
    val expectedOutputDF = createExpectedDataFrame(expected)
    assert(outputDataDF.collect sameElements expectedOutputDF.collect)
    val csvFileOutputDF = createCsvOutputDataFrame
    assert(csvFileOutputDF.collect sameElements expectedOutputDF.collect)
    displayData(expectedDF = expectedOutputDF, printLabel)
  }

  def aFailureIsGeneratedBy[T](expression: => T): Boolean =
    try {
      expression
      false
    } catch {
      case _: Throwable => true
    }

  private def displayData(expectedDF: DataFrame, displayLabel: String): Unit = {
    println("Compare Rows")
    println(s"Expected $displayLabel Output")
    expectedDF.show()
    println(s"Scala $displayLabel output")
    outputDataDF.show()
  }
}
