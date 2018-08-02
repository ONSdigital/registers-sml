package uk.gov.ons.registers

import java.nio.file.Path

import org.apache.spark.sql.DataFrame

import uk.gov.ons.registers.helpers.CSVProcessor.FilePath

// TODO - move to registers package
package object stepdefs {
  var inputPath: FilePath = _
  var outputPath: FilePath = _
  var stratificationPropertiesPath: FilePath = _
  var outputDataDF: DataFrame = _

  var stratifiedFramePath: Path = _
  var stratificationPropsPath: Path = _
}
