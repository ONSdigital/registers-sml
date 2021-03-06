package uk.gov.ons.registers.stepdefs.admin_data

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, StringType}
import uk.gov.ons.registers.methods.PayeCalculator
import uk.gov.ons.registers.stepdefs._
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, _}
import uk.gov.ons.stepdefs.Helpers

class PAYE_Steps extends ScalaDsl with EN with PayeCalculator {

  private def applyMethod(): Unit = {
    implicit val sparkSession = Helpers.sparkSession
    outputDataDF = calculatePAYE(BIDF, payeDF)
  }

  Given("""^the Legal unit input:"""){ inputTable: RawDataTableList =>
    BIDF = createDataFrame(inputTable)
      .withColumn(colName = "payerefs", split(col("payerefs"), ", ").cast(ArrayType(StringType)))
  }

  And("""^the PAYE refs input"""){ inputTable: RawDataTableList =>
    payeDF = toNull(createDataFrame(inputTable))
  }

  And("""^a PAYE refs input"""){ anInvalidFrameTableDF: RawDataTableList =>
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
    val output = assertDataFrameEquality(theExpectedResult)(castExpectedMandatoryFields = castWithPayeUnitMandatoryFields)
    displayData(expectedDF = output, printLabel = "PAYE")
  }

  Then("""an exception in Scala is thrown for .+ upon trying to Calculate PAYE$"""){ () =>
    assertThrown()
  }
}

