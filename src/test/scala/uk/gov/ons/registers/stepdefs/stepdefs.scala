package uk.gov.ons.registers

import org.apache.spark.sql.DataFrame

package object stepdefs {
  var frameDF: DataFrame = _
  var stratifiedFrameDF: DataFrame = _
  var stratificationPropsDF: DataFrame = _

  var outputDataDF: DataFrame = _

  var methodResult: Option[Exception] = _
}
