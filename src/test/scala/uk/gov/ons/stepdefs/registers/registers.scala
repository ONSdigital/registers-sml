package uk.gov.ons.stepdefs

import org.apache.spark.sql.DataFrame

package object registers {
  var inputPath: String = _
  var outputPath: String = _
  var stratificationPropertiesPath: String = _

//  var inputData: DataFrame = _
  var outputData: DataFrame = _
//  var stratProps: DataFrame = _
//  var expectedData: DataFrame = _
//
//  var startingPoint: BigDecimal = _
//  var outputSize: Int = _
}
