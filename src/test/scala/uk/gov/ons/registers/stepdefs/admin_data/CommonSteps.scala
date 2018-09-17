package uk.gov.ons.registers.stepdefs.admin_data

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, StringType}
import uk.gov.ons.registers.stepdefs.{BIDF, payeDF}
import uk.gov.ons.registers.support.AssertionHelpers.assertThrown
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame, toNull}

class CommonSteps extends ScalaDsl with EN{

  Given("""^the BI data input"""){ inputTable: RawDataTableList =>
    BIDF = createDataFrame(inputTable)
      .withColumn("PayeRefs", regexp_replace(col("PayeRefs"), "[\\[\\]]+", ""))
      .withColumn("VatRefs", regexp_replace(col("VatRefs"), "[\\[\\]]+", ""))

    BIDF = BIDF.withColumn(colName = "PayeRefs", split(col("PayeRefs"), ", ").cast(ArrayType(StringType)))
      .withColumn(colName = "VatRefs", split(col("VatRefs"), ", ").cast(ArrayType(StringType)))
  }

  And("""^the PAYE refs input"""){ inputTable: RawDataTableList =>
    payeDF = toNull(createDataFrame(inputTable))
  }

  Then("""an exception in Scala is thrown for .+ upon trying to (?:Calculate PAYE|Calculate VAT)$"""){ () =>
    assertThrown()
  }
}
