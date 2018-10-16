package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.methods.Stratification
import uk.gov.ons.registers.support.AssertionHelpers.{aFailureIsGeneratedBy, assertDataFrameEquality, displayData}
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, castWithUnitMandatoryFields, createDataFrame}
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.scala.{EN, ScalaDsl}

class StratificationSteps extends ScalaDsl with EN {
  private def stratifyTestFrame(): Unit =
    outputDataDF = Stratification.stratification(sparkSession = Helpers.sparkSession)
      .stratify(frameDF, stratificationPropsDF)

  Given("""a Frame:$"""){ aTranformedFrameTableDf: RawDataTableList =>
    frameDF = createDataFrame(aTranformedFrameTableDf)
  }

  Given("""a Frame with an invalid required field:$"""){ anInvalidFrameTableDf: RawDataTableList =>
    frameDF = createDataFrame(anInvalidFrameTableDf)
  }

  Given("""a Frame where some units have PayeEmployee field as null:$"""){ aFrameTableWithNull: RawDataTableList =>
    frameDF = createDataFrame(aFrameTableWithNull)
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

  Then("""a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata.*?:$"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExepctedMandatoryFields = castWithUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Stratification")
  }

  Then("""a Stratified Frame is returned with units assigned a Strata number, where the unit has a null PayeEmployee value the unit is allocated a Strata number of -2:$"""){
    theExpectedResultWithNull: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResultWithNull)(castExepctedMandatoryFields = castWithUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Stratification")
  }
}
