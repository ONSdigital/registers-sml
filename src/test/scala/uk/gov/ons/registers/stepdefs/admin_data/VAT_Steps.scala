package uk.gov.ons.registers.stepdefs.admin_data

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, StringType}
import uk.gov.ons.registers.methods.VAT
import uk.gov.ons.registers.stepdefs._
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation._
import uk.gov.ons.stepdefs.Helpers
import uk.gov.ons.registers.model.CommonFrameDataFields._

class VAT_Steps extends ScalaDsl with EN {
  private def applyMethod(): Unit = {
    outputDataDF = VAT.Vat(sparkSession = Helpers.sparkSession)
      .calculate(BIDF, payeDF, VatDF, appConfs)
  }

  Given("""^the Legal unit input with vat:"""){ inputTable: RawDataTableList =>
    BIDF = createDataFrame(inputTable)
      .withColumn("PayeRefs", regexp_replace(col("PayeRefs"), "[\\[\\]]+", ""))
      .withColumn("VatRefs", regexp_replace(col("VatRefs"), "[\\[\\]]+", ""))

    BIDF = BIDF.withColumn(colName = "PayeRefs", split(col("PayeRefs"), ", ").cast(ArrayType(StringType)))
      .withColumn(colName = "VatRefs", split(col("VatRefs"), ", ").cast(ArrayType(StringType)))
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

  When("""^Group Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, group_turnover)
  }

  When("""^Apportioned Turnover is calculated"""){ () =>
    applyMethod()
    outputDataDF = outputDataDF.na.fill(value = "").select(ern, apportioned)
  }

  When("""the VAT method is attempted$"""){ () =>
    methodResult = aFailureIsGeneratedBy {
      applyMethod()
    }
  }

  Then("""^a VAT results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExepctedMandatoryFields = castWithVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")  }

  Then("""^a Group Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExepctedMandatoryFields = castWithGroupVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }

  Then("""^an Apportioned Turnover results table is produced:"""){ theExpectedResult: RawDataTableList =>
    val output = assertDataFrameEquality(theExpectedResult)(castExepctedMandatoryFields = castWithAppVatUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "VAT")
  }

  Then("""an exception in Scala is thrown for .+ upon trying to Calculate VAT$"""){ () =>
    assertThrown()
  }
}
