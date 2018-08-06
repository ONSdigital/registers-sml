package uk.gov.ons.registers

import java.nio.file.Path

import org.apache.spark.sql.DataFrame

// TODO - move to registers package
package object stepdefs {
//  var inputPath: FilePath = _
  var outputPath: Path = _
//  var stratificationPropertiesPath: FilePath = _
  var outputDataDF: DataFrame = _

  var framePath: Path = _
  var stratifiedFramePath: Path = _
  var stratificationPropsPath: Path = _
}
