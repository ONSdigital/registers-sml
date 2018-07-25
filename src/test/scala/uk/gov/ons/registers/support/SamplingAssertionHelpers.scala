package uk.gov.ons.registers.support

import uk.gov.ons.registers.method.Sample
import uk.gov.ons.registers.stepdefs.{inputPath, outputDataDF, outputPath, stratificationPropertiesPath}

object SamplingAssertionHelpers {

  def makeSample(): Unit =
    outputDataDF = Sample.sample(inputPath)
      .create(stratificationPropertiesPath, outputPath)

}
