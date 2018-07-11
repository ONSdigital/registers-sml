package uk.gov.ons.stepdefs.registers

import cucumber.api.scala.{EN, ScalaDsl}
import uk.gov.ons.models.StratificationProperties.{startPoint, sampleSize}


class SamplingSteps extends ScalaDsl with EN {

  When("""the Scala sampling method is ran on the pre-filtered input"""){ () =>
    startingPoint = stratProps.rdd.first().getAs(startPoint)
    outputSize = stratProps.rdd.first().getAs(sampleSize)
    outputData = createSample(inputData)
  }

  Then("""a DataFrame of sample size is returned"""){ () =>
    expectedData = ???
//    outputData.map(row => assert(row.getAs[Double](prn) >= startingPoint))
    assert(outputData.collect() sameElements expectedData.collect())
    assert(outputData.count == 1100L)

    println("Input Data")
    println("Expected Output")
    expectedData.show()
    println("Scala Melt output")
    outputData.show()
  }

}
