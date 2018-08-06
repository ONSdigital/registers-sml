package uk.gov.ons.registers

import java.nio.file.Path

import org.apache.spark.sql.DataFrame

package object stepdefs {
  var outputPath: Path = _
  var outputDataDF: DataFrame = _

  var framePath: Path = _
  var stratifiedFramePath: Path = _
  var stratificationPropsPath: Path = _
}
