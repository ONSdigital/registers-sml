package uk.gov.ons.registers.stepdefs

import scala.collection.JavaConversions._

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import uk.gov.ons.registers.methods.Sample
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.support.FileProcessorHelper._
import uk.gov.ons.registers.support.TestFileEnvSetup
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}


class SamplingSteps extends ScalaDsl with EN{

  import Helpers.sparkSession

  private def createDataFrame(aListOfLines: Seq[List[String]]): DataFrame = {
    val rows = aListOfLines.drop(1).map(Row.fromSeq(_))
    val rdd = sparkSession.sparkContext.makeRDD(rows)
    val fieldTypes = aListOfLines.head.map(StructField(_, dataType = StringType, nullable = false))
    sparkSession.createDataFrame(rdd, StructType(fieldTypes))
  }

  private def createExpectedDataFrame(dataTable: DataTable): DataFrame = {
    val aListOfExpectedRows = dataTable.asLists(classOf[String])
    createDataFrame(aListOfLines = aListOfExpectedRows.toList.map(_.toList))
  }

  private def createCsvOutputDataFrame: DataFrame = {
    val csvOutput = assertAndReturnCsvOfSampleCollection(outputPath = outputPath)
    val csvFileAsLists = lineAsListOfFields(file = csvOutput)
    createDataFrame(aListOfLines = csvFileAsLists)
  }

  private def assertDataFrameEquality(expected: DataTable): Unit = {
    val expectedOutputDF = createExpectedDataFrame(expected)
    assert(outputDataDF.collect sameElements expectedOutputDF.collect)
    val csvFileOutputDF = createCsvOutputDataFrame
    assert(csvFileOutputDF.collect sameElements expectedOutputDF.collect)
    displayData(expectedDF = expectedOutputDF)
  }

  private def createSampleTest(): Unit = {
    outputPath = TestFileEnvSetup.createTempDirectory(prefix = "test_output_")
    outputDataDF = Sample.sample(inputPath = stratifiedFramePath)(sparkSession = Helpers.sparkSession)
      .create(stratificationPropsPath, outputPath)
  }

  When("""a Scala Sample is created from a Stratified Frame"""){ () =>
    createSampleTest()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""an exception in Scala is thrown for Stratified [^\s]+ not being found upon trying to Sample"""){ () =>
    val expectedFailureException = try {
      createSampleTest()
      false
    } catch {
      case _: Exception => true
      case _ => false
    }
    assert(expectedFailureException)
  }

  Then("""a Sample containing the Census strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult)
  }

  Then("""a Sample containing the Prn-Sample strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult)
  }

  Then("""a Sample is returned and exported to CSV with the inclusion of stratas with outbound Sample Size parameter"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult)
  }

  Then("""a Sample is returned and exported to CSV with the strata containing an invalid Sample Size is logged"""){ theExpectedResult: DataTable =>
    // TODO test log
    assertDataFrameEquality(expected = theExpectedResult)
  }
}
