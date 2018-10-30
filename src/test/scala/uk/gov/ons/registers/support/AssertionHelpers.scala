package uk.gov.ons.registers.support

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.types.IntegerType

import uk.gov.ons.registers.Patch
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber
import uk.gov.ons.registers.stepdefs.{methodResult, outputDataDF}
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame, nullToNull}

object AssertionHelpers{
  def assertDataFrameEquality(expected: RawDataTableList)(castExpectedMandatoryFields: DataFrame => DataFrame): DataFrame = {
    val expectedOutputDF = nullToNull(createDataFrame(expected))
    val castedExpectedOutputDF = castExpectedMandatoryFields(expectedOutputDF)

    assert(outputDataDF.collect sameElements castedExpectedOutputDF.collect, s"the output dataframe " +
      s"[${outputDataDF.collect.toList}] was not equal to expected output dataframe [${castedExpectedOutputDF.collect.toList}]")
    castedExpectedOutputDF
  }

  def assertDataFrameStringEquality(expected: RawDataTableList, bounds: String)(castExepctedMandatoryFields: (DataFrame, String) => DataFrame): DataFrame = {
    val expectedOutputDF = createDataFrame(expected)
    val castedExpectedOutputDF = castExepctedMandatoryFields(expectedOutputDF.withColumn(colName = cellNumber, expectedOutputDF.col(cellNumber).cast(IntegerType)), bounds)

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
        Patch.log(msg = s"Found expected error: ${ex.getMessage}")
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
}
