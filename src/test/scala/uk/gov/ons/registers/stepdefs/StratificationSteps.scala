package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.method.Stratification
import uk.gov.ons.registers.support.AssertionHelpers.{assertAndReturnCsvOfSampleCollection, assertNewCellNumberFieldHasBeenAdded, assertSampleCollectionSize, _}

import cucumber.api.scala.{EN, ScalaDsl}

class StratificationSteps extends ScalaDsl with EN {

  When("""a Scala Stratified Frame is created from the pre-filtered Frame"""){ () =>
    outputDataDF = Stratification.stratification(inputPath = inputPath)
      .stratify(stratificationPropsPath = stratificationPropertiesPath, outputPath = outputPath)
  }

  Then("""a Stratified Frame for all given stratas is returned and exported to CSV"""){ () =>

    val employeeAndSic07StatifiedFrameCsvFile = assertAndReturnCsvOfSampleCollection
    assertSampleCollectionSize(sampleCollectionCsv = employeeAndSic07StatifiedFrameCsvFile, expectedNumberOfRecords = 64)
    assertNewCellNumberFieldHasBeenAdded(employeeAndSic07StatifiedFrameCsvFile)

    // test row
    // test inclusive


    displayData(expectedRow = outputDataDF.first) // TODO CHANGE row here to expected
  }
}
