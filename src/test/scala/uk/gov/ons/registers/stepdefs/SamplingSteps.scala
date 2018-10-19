//package uk.gov.ons.registers.stepdefs
//
//import scala.collection.JavaConverters.asScalaBufferConverter
//
//import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.spark.sql.{DataFrame, Row}
//
//import uk.gov.ons.registers.methods.Sample
//import uk.gov.ons.registers.support.AssertionHelpers.{aFailureIsGeneratedBy, assertDataFrameEquality, displayData}
//import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, castWithStratifiedUnitMandatoryFields, createDataFrame}
//import uk.gov.ons.stepdefs.Helpers
//import uk.gov.ons.stepdefs.Helpers.sparkSession
//
//import cucumber.api.scala.{EN, ScalaDsl}
//
//class SamplingSteps extends ScalaDsl with EN{
//  private def assertEqualityAndPrintResults(expected: RawDataTableList): Unit = {
//    val output = assertDataFrameEquality(expected)(castExepctedMandatoryFields = castWithStratifiedUnitMandatoryFields)
//    displayData(expectedDF = output, printLabel = "Sampling")
//  }
//
//  private def createSampleTest(): Unit =
//    outputDataDF = Sample.sample(sparkSession = Helpers.sparkSession)
//      .create(stratifiedFrameDF, stratificationPropsDF)
//
//  Given("""a Stratified Frame:$"""){ aFrameTable: RawDataTableList =>
//    stratifiedFrameDF = createDataFrame(aFrameTable)
//  }
//
//  Given("""a Stratified Frame with an invalid required field:$"""){ anInvalidFrameTable: RawDataTableList =>
//    stratifiedFrameDF = createDataFrame(anInvalidFrameTable)
//  }
//
//  When("""a Scala Sample is created from a Stratified Frame$"""){ () =>
//    createSampleTest()
//    outputDataDF = outputDataDF.na.fill(value = "")
//  }
//
//  When("""a Sample creation is attempted$"""){ () =>
//    methodResult = aFailureIsGeneratedBy {
//      createSampleTest()
//    }
//  }
//
//  Then("""a Sample containing the Sample \w+ from the .+ strata is returned:$"""){ theExpectedResult: RawDataTableList =>
//    assertEqualityAndPrintResults(expected = theExpectedResult)
//  }
//
//  Then("""a Sample containing the total population in the strata is returned:$"""){ theExpectedResult: RawDataTableList =>
//    // TODO test log
//    assertEqualityAndPrintResults(expected = theExpectedResult)
//  }
//}
