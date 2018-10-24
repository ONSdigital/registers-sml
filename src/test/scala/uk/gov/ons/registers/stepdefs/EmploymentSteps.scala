package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.registers.methods.EmploymentCalculator
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._
import uk.gov.ons.stepdefs.Helpers


class EmploymentSteps extends ScalaDsl with EN with EmploymentCalculator {

  private def applyMethod(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = calculateEmployment(empDF)
    //outputDataDF.show()
  }

  Given("""^an employees and working proprietors input:$"""){ inputTable: RawDataTableList =>
    empDF = toNull(createDataFrame(inputTable))
  }

  When("""^employment is calculated$"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""^the employment calculation is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^this Employment table is is produced$"""){ theExpectedResult: RawDataTableList =>
    //createDataFrame(theExpectedResult).show
    val output = assertDataFrameEquality(theExpectedResult)(castExepctedMandatoryFields = castWithEmploymentUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Employment")
  }

  Then("""^an exception in Scala is thrown for Frame due to a mismatch field type upon trying to Calculate employment$"""){ () =>
    assertThrown()
  }
}

