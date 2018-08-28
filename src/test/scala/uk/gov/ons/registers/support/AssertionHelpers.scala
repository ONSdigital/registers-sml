package uk.gov.ons.registers.support

import java.io.File
import java.nio.file.Path

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.TestLogPatch
import uk.gov.ons.registers.helpers.CSVProcessor.CSV
import uk.gov.ons.registers.stepdefs.{methodResult, outputDataDF}
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame}

object AssertionHelpers{
  def assertDataFrameEquality(expected: RawDataTableList)(castExepctedMandatoryFields: DataFrame => DataFrame): DataFrame = {
    val expectedOutputDF = createDataFrame(expected)
    val castedExpectedOutputDF = castExepctedMandatoryFields(expectedOutputDF)

    assert(outputDataDF.collect sameElements castedExpectedOutputDF.collect, s"the output dataframe " +
      s"[${outputDataDF.collect.toList}] was not equal to expected output dataframe [${castedExpectedOutputDF.collect.toList}]")
    castedExpectedOutputDF
  }
  def aFailureIsGeneratedBy[T](expression: => T): Option[Exception] =
    try {
      expression
      None
    } catch {
      case ex: Throwable =>
        TestLogPatch.log(msg = s"Found expected error: ${ex.getMessage}")
        Some(new Exception(ex.getMessage))
    }

  def assertThrown(): Unit =
    assert(methodResult.fold(false)(_ => true), "expected Exception to be thrown from running method with an invalid argument")

  def displayData(expectedDF: DataFrame, printLabel: String): Unit = {
    println("Compare Rows")
    println(s"Expected $printLabel Output")
    expectedDF.show()
    println(s"Scala $printLabel Output")
    outputDataDF.show()
  }

  @deprecated
  def assertAndReturnCsvOfSampleCollection(outputPath: Path): File = {
    val sampleOutputDir = outputPath.toFile
    assert(sampleOutputDir.exists && sampleOutputDir.isDirectory, message = s"output path [$outputPath] does not exist and/ or is not a directory")
    val listOfCsvOutputFiles = sampleOutputDir.listFiles.filter(_.getName.endsWith(s".$CSV"))
    assert(listOfCsvOutputFiles.nonEmpty, message = s"found no files with extension [.$CSV] in [$outputPath] directory")
    listOfCsvOutputFiles.head
  }
}
