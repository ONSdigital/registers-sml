package uk.gov.ons.registers.stepdefs


import cucumber.api.scala.{EN, ScalaDsl}

import uk.gov.ons.registers.methods.EmployeesCalculator
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._
import uk.gov.ons.stepdefs.Helpers

class EmployeesSteps extends ScalaDsl with EN with EmployeesCalculator{

  private def applyMethod(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = calculateEmployees(empDF)
  }

  Given("""^an employees input:$"""){ inputTable: RawDataTableList =>
    empDF = toNull(createDataFrame(inputTable))
  }

  When("""^employees is calculated$"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""^the employees calculation is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^this Employees table is is produced$"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithEmployeesUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Employees")
  }

  Then("""^an exception in Scala is thrown for Frame due to a mismatch field type upon trying to Calculate employees$"""){ () =>
    assertThrown()
  }
}
