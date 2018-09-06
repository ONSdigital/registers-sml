package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.support.AssertionHelpers.assertThrown
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame, toNull}
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.functions.{col, regexp_replace, split}
import org.apache.spark.sql.types.{ArrayType, StringType}

class CommonSteps extends ScalaDsl with EN {

  Given("""^the BI data input:$"""){ inputTable: RawDataTableList =>
    BIDF = createDataFrame(inputTable)
      .withColumn("PayeRefs", regexp_replace(col("PayeRefs"), "[\\[\\]]+", ""))
      .withColumn("VatRefs", regexp_replace(col("VatRefs"), "[\\[\\]]+", ""))

    BIDF = BIDF.withColumn(colName = "PayeRefs", split(col("PayeRefs"), ", ").cast(ArrayType(StringType)))
      .withColumn(colName = "VatRefs", split(col("VatRefs"), ", ").cast(ArrayType(StringType)))
  }

  And("""a Strata(?:|s| of selection type (?:Prn-Sampling|Census)) from Stratification Properties file:$"""){ aPropertiesTable: RawDataTableList =>
    stratificationPropsDF = createDataFrame(aPropertiesTable)
  }

  And("""a Stratification Properties file with an invalid field type:$"""){ anInvalidPropertiesTable: RawDataTableList =>
    stratificationPropsDF = createDataFrame(anInvalidPropertiesTable)
  }

  And("""^the PAYE refs input"""){ inputTable: RawDataTableList =>
    payeDF = toNull(createDataFrame(inputTable))
  }

  Then("""an exception in Scala is thrown for .+ due to a mismatch field type upon trying to (?:Sample|Stratify|Calculate PAYE|Calculate VAT)$"""){ () =>
    assertThrown()
  }
}
