package uk.gov.ons.registers.support

import java.io.File

import org.apache.spark.sql.Row

import uk.gov.ons.registers.helpers.CSVProcessor.CSV
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields.cellNumber
import uk.gov.ons.registers.stepdefs.{outputDataDF, outputPath}
import uk.gov.ons.registers.support.FileProcessorHelper.{getFileHeader, getFileLength}
//import org.junit.Assert._

object AssertionHelpers {
  // Assert if CSV file is saved - distributed, and thus cannot use fixed naming match
  def assertAndReturnCsvOfSampleCollection: File = {
    val sampleOutputDir = new java.io.File(outputPath)
    assert(sampleOutputDir.exists && sampleOutputDir.isDirectory, message = s"output path [$outputPath] does not exist and/ or is not a directory")
    val listOfCsvOutputFiles = sampleOutputDir.listFiles.filter(_.getName.endsWith(s".$CSV"))
    assert(listOfCsvOutputFiles.nonEmpty, message = s"found no files with extension [.$CSV] in [$outputPath] directory")
    listOfCsvOutputFiles.head
  }

  // Assert if result DataFrame and csv output produces the right number of records in total
  def assertSampleCollectionSize(sampleCollectionCsv: File, expectedNumberOfRecords: Long): Unit = {
    assert(outputDataDF.count == expectedNumberOfRecords,
      message = s"expected DataFrame size [${outputDataDF.count}] did not equal actual size [$expectedNumberOfRecords]")
    val numberOfRowsInFile = getFileLength(sampleCollectionCsv)
    assert(numberOfRowsInFile == expectedNumberOfRecords,
      message = s"expected CSV sample(s) size [$numberOfRowsInFile] did not equal actual size [$expectedNumberOfRecords]")
  }

  // Assert if new df schema and CSV output has new column - request identifier (cell_no)
  def assertNewCellNumberFieldHasBeenAdded(sampleCollectionCsv: File): Unit = {
    val dfSchema = outputDataDF.schema.fieldNames
    assert(dfSchema contains cellNumber, message = s"expected field [$cellNumber] could not be found in DataFrame field names schema")
    val csvHeaders = getFileHeader(sampleCollectionCsv)
    assert(csvHeaders contains cellNumber, message = s"expected field [$cellNumber] could not be found in CSV header")
  }

  def assertRowEquality(csvRow: Row, dfRow: Row = outputDataDF.first, expectedRow: Row): Unit = {
    assert(RowEqualitySupport.equals(actualRow=csvRow, expectedRow=expectedRow),
      message = s"actual csv row stored [$csvRow] did not equal expected row [$expectedRow]")
    // TODO - Resolve and Uncomment i.e. space at the end of postcode
//    assert(RowEqualitySupport.equals(actualRow=dfRow, expectedRow=expectedRow),
//      message = s"actual DataFrame first row [$dfRow] did not equal expected row [$expectedRow]")
  }

  def displayData(expectedRow: Row): Unit = {
    println("Compare Rows")
    println("Expected First Row Output")
    println(expectedRow)
    println("Actual First Row [DF] Output")
    println(outputDataDF.first)

    println("Scala Sampling output")
    outputDataDF.show()
  }
}
