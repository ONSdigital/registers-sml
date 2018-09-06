package uk.gov.ons.registers.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import uk.gov.ons.registers.utils.DataTableTransformation._
import uk.gov.ons.registers.methods.PAYE
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, toNull}

class PAYE_Steps extends ScalaDsl with EN {

  private def assertEqualityAndPrintResults(expected: RawDataTableList): Unit = {
  val output = assertDataFrameEquality(expected)(castExepctedMandatoryFields = castWithPayeUnitMandatoryFields)
  displayData(expectedDF = output, printLabel = "PAYE")
}

  private def applyMethod(): Unit = {
  outputDataDF = PAYE.Paye(sparkSession = Helpers.sparkSession)
    .calculate(BIDF, payeDF, appConfs)
  }

  Given("""^a BI data input with field that does not exist:$"""){ anInvalidFrameTableDF: RawDataTableList =>
    BIDF = createDataFrame(anInvalidFrameTableDF)
      .withColumn("PayeRefs", regexp_replace(col("PayeRefs"), "[\\[\\]]+", ""))
      .withColumn("VatRefs", regexp_replace(col("VatRefs"), "[\\[\\]]+", ""))

    BIDF = BIDF.withColumn(colName = "PayeRefs", split(col("PayeRefs"), ", ").cast(ArrayType(StringType)))
      .withColumn(colName = "VatRefs", split(col("VatRefs"), ", ").cast(ArrayType(StringType)))
  }

  And("""^a PAYE refs input with invalid field"""){ anInvalidFrameTableDF: RawDataTableList =>
    payeDF = createDataFrame(anInvalidFrameTableDF)
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
    assertEqualityAndPrintResults(expected = theExpectedResult)
  }

}

