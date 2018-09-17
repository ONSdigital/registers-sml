package uk.gov.ons.registers.stepdefs.admin_data

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types._
import uk.gov.ons.registers.methods.PAYE
import uk.gov.ons.registers.stepdefs._
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, _}
import uk.gov.ons.stepdefs.Helpers

class PAYE_Steps extends ScalaDsl with EN {

  private def assertEqualityAndPrintResults(expected: RawDataTableList): Unit = {
  val output = assertDataFrameEquality(expected)(castExepctedMandatoryFields = castWithPayeUnitMandatoryFields)
  displayData(expectedDF = output, printLabel = "PAYE")
}

  private def applyMethod(): Unit = {
  outputDataDF = PAYE.Paye(sparkSession = Helpers.sparkSession)
    .calculate(BIDF, payeDF, appConfs)
    //outputDataDF.show()
  }

  And("""^a PAYE refs input with"""){ anInvalidFrameTableDF: RawDataTableList =>
    payeDF = toNull(createDataFrame(anInvalidFrameTableDF))
  }

  When("""^the PAYE method is applied$"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""the PAYE method is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^a PAYE results table is produced:"""){ theExpectedResult: RawDataTableList =>
    //createDataFrame(theExpectedResult).show
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }

}

