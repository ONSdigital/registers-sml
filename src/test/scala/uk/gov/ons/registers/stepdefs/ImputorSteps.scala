package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame}
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.methods.Imputor
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._

class ImputorSteps extends ScalaDsl with EN{

  val imputor = new Imputor(){}

  private def applyMethod() = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = imputor.imputeTurnoverAndEmpees(frameDF, frameAndDF)
  //  outputDataDF.show()
//    outputDataDF.printSchema()
  }

  Given("""an employees """){ inputDF: RawDataTableList =>
    frameDF = toNull(createDataFrame(inputDF))
  }

  And(""".+ TPH input:$"""){ inputDF: RawDataTableList =>
    frameAndDF = toNull(createDataFrame(inputDF))
  }

  When("""the Imputor is run$""") { () =>
    applyMethod()
  }

  When("""the Imputor is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""the Imputed results table is produced:$"""){ theExpectedDF: RawDataTableList =>
//    createDataFrame(theExpectedDF).show()
//    createDataFrame(theExpectedDF).printSchema()
    val output = assertDataFrameEquality(theExpectedDF)(castExpectedMandatoryFields = castWithImputedUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "Imputed")
  }

  Then("""an exception in Scala is thrown for Frame due to a mismatch field type upon trying to Impute$"""){ () =>
    assertThrown()
  }
}
