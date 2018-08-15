package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.methods.Stratification
import uk.gov.ons.registers.support.AssertionHelpers.{aFailureIsGeneratedBy, assertDataFrameEquality, displayData}
import uk.gov.ons.registers.utils.DataTableExportUtil.saveTableAsCsv
import uk.gov.ons.registers.support.TestFileEnvSetup.createAPath
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class StratificationSteps extends ScalaDsl with EN {
  private def assertEqualityAndPrintResults(expected: DataTable): Unit =
    displayData(expectedDF = assertDataFrameEquality(expected), printLabel = "Stratification")

  private def stratifyTestFrame(): Unit =
    outputDataDF = Stratification.stratification(inputPath = framePath)(sparkSession = Helpers.sparkSession)
      .stratify(stratificationPropsPath = stratificationPropsPath, outputPath = outputPath)

  Given("""a Frame:$"""){ aFrameTable: DataTable =>
    framePath = saveTableAsCsv(
      dataTable = aFrameTable,
      prefix = "frame_")
  }

  Given("""a Frame does not exist$"""){ () =>
    framePath = createAPath(pathStr = "invalid_frame_path")
  }

  When("""a Scala Stratified Frame is created from a Frame$"""){ () =>
    stratifyTestFrame()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""a Stratified Frame creation is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      stratifyTestFrame()
    }
  }

  Then("""a Stratified Frame is returned and exported to CSV with the strata assigned the Strata number from the Stratification Strata.*?:$"""){ theExpectedResult: DataTable =>
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }
}
