package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import uk.gov.ons.registers.methods.VAT
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame, toNull}
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._

class VAT_Steps extends ScalaDsl with EN {
  private def assertEqualityAndPrintResults(expected: RawDataTableList): Unit = {
    val output = assertDataFrameEquality(expected)(castExepctedMandatoryFields = castWithVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }
  private def applyMethod(): Unit = {
    outputDataDF = VAT.Vat(sparkSession = Helpers.sparkSession)
      .calculate(BIDF, payeDF, VatDF, appConfs)
  }

  And("""^the VAT refs input"""){ inputTable: RawDataTableList =>
    VatDF = toNull(createDataFrame(inputTable))
  }

  And("""^a VAT refs input with a """){ anInvalidFrameTableDF: RawDataTableList =>
    VatDF = createDataFrame(anInvalidFrameTableDF)
  }

  When("""^VAT is calculated$"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "")
  }

  When("""the VAT method is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^a VAT results table is produced:"""){ theExpectedResult: RawDataTableList =>
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }








}
