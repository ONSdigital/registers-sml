package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.methods.Sample
import uk.gov.ons.registers.support.AssertionHelpers.{aFailureIsGeneratedBy, assertDataFrameEquality, displayData}
import uk.gov.ons.registers.support.DataTableExportUtil.saveTableAsCsv
import uk.gov.ons.registers.support.TestFileEnvSetup.createAPath
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class SamplingSteps extends ScalaDsl with EN{
  private def assertEqualityAndPrintResults(expected: DataTable): Unit =
    displayData(expectedDF = assertDataFrameEquality(expected), printLabel = "Sampling")

  private def createSampleTest(): Unit =
    outputDataDF = Sample.sample(stratifiedFramePath)(sparkSession = Helpers.sparkSession)
      .create(stratificationPropsPath, outputPath)

  Given("""a Stratified Frame:$"""){ aFrameTable: DataTable =>
    stratifiedFramePath = saveTableAsCsv(
      dataTable = aFrameTable,
      prefix = "stratified_frame_")
  }

  Given("""a Stratified Frame does not exist$"""){ () =>
    stratifiedFramePath = createAPath(pathStr = "invalid_stratified_frame_path")
  }

  When("""a Scala Sample is created from a Stratified Frame$"""){ () =>
    createSampleTest()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""a Sample creation is attempted$"""){ () =>
    methodFailureFlag = aFailureIsGeneratedBy {
      createSampleTest()
    }
  }

  Then("""a Sample containing the Sample \w+ from the .+ strata is returned and exported to CSV:$"""){ theExpectedResult: DataTable =>
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }

  Then("""a Sample containing the total population in the strata is returned and exported to CSV:$"""){ theExpectedResult: DataTable =>
    // TODO test log
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }
}
