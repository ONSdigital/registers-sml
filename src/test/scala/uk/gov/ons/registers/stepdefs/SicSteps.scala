package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.registers.methods.Sic
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, _}
import uk.gov.ons.stepdefs.Helpers

class SicSteps extends ScalaDsl with EN {

  val sic = new Sic(){}

  private def applyMethod(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = sic.getClassification(inputDF)
  }

  Given("""^input:"""){ inputTable: RawDataTableList =>
    inputDF = createDataFrame(inputTable)
  }

  When("""^the Sic method is calculated"""){ () =>
    applyMethod()
  }

  When("""^the Sic method is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^the Sic results table is produced:$""") { theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithSicUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "SIC")
  }

  Then("""an exception in Scala is thrown for .+ upon trying to calculate Sic$"""){ () =>
    assertThrown()
  }
}
