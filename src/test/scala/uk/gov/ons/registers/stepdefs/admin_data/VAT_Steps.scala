package uk.gov.ons.registers.stepdefs.admin_data

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, StringType}
import uk.gov.ons.registers.methods.VatCalculator
import uk.gov.ons.registers.stepdefs._
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.model.CommonFrameDataFields._

class VAT_Steps extends ScalaDsl with EN with VatCalculator {
  private def applyMethod(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = calculateVAT(BIDF, payeDF, VatDF)
  }

  Given("""^the Legal unit input with vat:"""){ inputTable: RawDataTableList =>
    BIDF = createDataFrame(inputTable)
        .withColumn(colName = "payerefs", split(col("payerefs"), ", ").cast(ArrayType(StringType)))
        .withColumn(colName = "vatrefs", split(col("vatrefs"), ", ").cast(ArrayType(StringType)))
  }

  And("""^the PAYE input:"""){ inputTable: RawDataTableList =>
    payeDF = toNull(createDataFrame(inputTable))
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

  When("""^Contained Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, contained)
  }

  When("""^Standard Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, standard)
  }

  When("""^Apportioned Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, apportioned)
  }

  When("""^Group Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, group_turnover)
  }

  When("""the VAT method is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^a combination of the PAYE and VAT results tables is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")  }

  Then("""^a Contained Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithCntVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }

  Then("""^a Standard Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithStdVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }
   Then("""^an Apportioned Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
     val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithAppVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }

  Then("""^a Group Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithGroupVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }

  Then("""an exception in Scala is thrown for .+ upon trying to Calculate VAT$"""){ () =>
    assertThrown()
  }
}
