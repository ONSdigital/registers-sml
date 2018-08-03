package uk.gov.ons.registers.stepdefs

import java.nio.file.Path

import uk.gov.ons.registers.methods.Sample
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.support.TestFileEnvSetup.{createAPath, createTempDirectory}
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}


class SamplingSteps extends ScalaDsl with EN{

  private val printLabel = "Sampling"

  private def createSampleTest(outputDirectoryPath: Option[Path] = None): Unit = {
    outputPath = outputDirectoryPath.getOrElse(createTempDirectory(prefix = "sample_test_output_"))
    outputDataDF = Sample.sample(stratifiedFramePath)(sparkSession = Helpers.sparkSession)
      .create(stratificationPropsPath, outputPath)
  }

  Given("""a Stratified Frame does not exist$"""){ () =>
    stratifiedFramePath = createAPath(pathStr = "invalid_stratified_frame_path")
  }

  When("""a Scala Sample is created from a Stratified Frame"""){ () =>
    createSampleTest()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""an exception in Scala is thrown for Stratified Properties not being found upon trying to Sample"""){ () =>
    assert(aFailureIsGeneratedBy {
      createSampleTest()
    })
  }

  When("""an exception in Scala is thrown for Stratified Frame not being found upon trying to Sample"""){ () =>
    assert(aFailureIsGeneratedBy {
      createSampleTest()
    })
  }

  Then("""a Sample containing the sample selection from the Census strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult, printLabel)
  }

  Then("""a Sample containing the Sample Size from the Prn-Sample strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult, printLabel)
  }

  Then("""a Sample containing the Sample Size from the PRN-sampling strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    assertDataFrameEquality(expected = theExpectedResult, printLabel)
  }

  Then("""a Sample containing the total population in the strata is returned and exported to CSV"""){ theExpectedResult: DataTable =>
    // TODO test log
    assertDataFrameEquality(expected = theExpectedResult, printLabel)
  }
}
