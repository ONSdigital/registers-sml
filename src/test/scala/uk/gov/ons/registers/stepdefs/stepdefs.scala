package uk.gov.ons.registers

import global.AppParams
import org.apache.spark.sql.DataFrame

package object stepdefs {
  var frameDF: DataFrame = _
  var stratifiedFrameDF: DataFrame = _
  var stratificationPropsDF: DataFrame = _

  var outputDataDF: DataFrame = _

  var BIDF: DataFrame = _
  var payeDF: DataFrame = _
  var VatDF: DataFrame = _
  var appConfs: AppParams = _

  var methodResult: Option[Exception] = _
}
