package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.registers.methods.Stratification
import uk.gov.ons.registers.model.CommonFrameDataFields.prn
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber
import uk.gov.ons.registers.support.AssertionHelpers.{aFailureIsGeneratedBy, assertDataFrameStringEquality, displayData}
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, castWithUnitMandatoryFields, createDataFrame}
import uk.gov.ons.stepdefs.Helpers

class StratificationSteps extends ScalaDsl with EN with Stratification {

  private def stratifyTestFrame(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = stratify(frameDF, stratificationPropsDF, bounds).orderBy(cellNumber, prn)
  }

  Given("""a Frame:$"""){ aTranformedFrameTableDf: RawDataTableList =>
    frameDF = createDataFrame(aTranformedFrameTableDf)
  }

  Given("""a Frame with an invalid required field:$"""){ anInvalidFrameTableDf: RawDataTableList =>
    frameDF = createDataFrame(anInvalidFrameTableDf)
  }

  Given("""a Frame where some units have PayeEmployee field as null:$"""){ aFrameTableWithNull: RawDataTableList =>
    frameDF = createDataFrame(aFrameTableWithNull)
  }

  And("""a specification of unit and params:"""){ frame: RawDataTableList =>
    bounds = createDataFrame(frame).head().getString(1)
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


  Then("""a Stratified Frame is returned"""){ theExpectedResult: RawDataTableList =>
    implicit val sparkSession = Helpers.sparkSession
    val output = assertDataFrameStringEquality(theExpectedResult, bounds)(castExepctedMandatoryFields = castWithUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Stratification")
  }
}
