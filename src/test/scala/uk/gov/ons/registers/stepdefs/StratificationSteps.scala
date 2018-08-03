package uk.gov.ons.registers.stepdefs

import java.nio.file.Path

import uk.gov.ons.registers.method.Stratification
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.support.TestFileEnvSetup.{createAPath, createTempDirectory}
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class StratificationSteps extends ScalaDsl with EN {

  private val printLabel = "Stratification"

  private def stratifyFrame(outputDirectoryPath: Option[Path] = None): Unit = {
    outputPath = outputDirectoryPath.getOrElse(createTempDirectory(prefix = "stratification_test_output_"))
    outputDataDF = Stratification.stratification(inputPath = framePath)(sparkSession = Helpers.sparkSession)
      .stratify(stratificationPropsPath = stratificationPropsPath, outputPath = outputPath)
  }

  Given("""a Frame does not exist$"""){ () =>
    framePath = createAPath(pathStr = "invalid_frame_path")
  }

  When("""a Scala Stratified Frame is created from a Frame"""){ () =>
    stratifyFrame()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  Then("""a Stratified Frame is returned and exported to CSV with the strata assigned the Strata number from the Stratification Strata"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult, printLabel)
  }

  When("""an exception in Scala is thrown for Frame not being found upon trying to Stratify"""){ () =>
    assert(aFailureIsGeneratedBy {
      stratifyFrame()
    })
  }

  When("""an exception in Scala is thrown for Stratified Properties not being found upon trying to Stratify"""){ () =>
    assert(aFailureIsGeneratedBy {
      stratifyFrame()
    })
  }
}
