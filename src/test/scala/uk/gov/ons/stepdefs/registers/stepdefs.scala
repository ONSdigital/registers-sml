package uk.gov.ons.stepdefs

import org.apache.spark.sql.DataFrame

package object registers {
  var inputData: DataFrame = _
  var outputData: DataFrame = _
  var stratProps: DataFrame = _
  var expectedData: DataFrame = _
  // NOTE - may lose precision
  var startingPoint: Double = _
  var outputSize: Long = _
}
