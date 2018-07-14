package uk.gov.ons.stepdefs.registers

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.method.Sample

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.registers.models.StratificationProperties.cellNumber


class SamplingSteps extends ScalaDsl with EN {

  When("""the Scala sampling method is ran on the pre-filtered input"""){ () =>
    outputData = Sample.sample(inputPath)
      .create(stratificationPropertiesPath, outputPath)
  }

  Then("""a DataFrame of given sample size is returned"""){ () =>
    val expectedData: DataFrame = ???
    assert(outputData.count == 20080L)
    assert(outputData.first() equals expectedData.first())
    assert(outputData.schema.fieldNames contains cellNumber)

    assert(new java.io.File(s"$outputPath/*.csv").exists)

    // check file has been created
    // check number of rows
    // check value has been appended at the end '*no'!!

    println("Compare DataFrames")
    println("Expected Output")
    expectedData.show()
    println("Scala Sampling output")
    outputData.show()
  }

}
