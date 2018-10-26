package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}

import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame}
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.methods.Imputor
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._

class ImputorSteps extends ScalaDsl with EN with Imputor {

  private def applyMethod() = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = imputeTurnoverAndEmpees(frameDF, frameAndDF)
    outputDataDF.show()
  }

  Given("""an employees and turnover input:$"""){ inputDF: RawDataTableList =>
    frameDF = createDataFrame(inputDF)
  }

  And("""TPH input:$"""){ inputDF: RawDataTableList =>
    frameAndDF = createDataFrame(inputDF)
  }

  When("""the Imputor is run$""") { () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "")
  }


  Then("""the results are:$"""){ theExpectedDF: RawDataTableList =>
    createDataFrame(theExpectedDF).show()
    val output = assertDataFrameEquality(theExpectedDF)(castExpectedMandatoryFields = castWithImputedUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Imputed")
  }
}
