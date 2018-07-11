package uk.gov.ons.stepdefs.registers

import java.util

import org.apache.spark.sql.DataFrame

import uk.gov.ons.stepdefs.Helpers

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSamplingSteps extends ScalaDsl with EN {

  private object ParsingUtils {
    private val CSV: String = "csv"
    private val endsWith: String => Boolean = (file: String) => file.endsWith(CSV)
    private val filePaths = 0

    val inputPath: String = "data_input_path"
    val outputPath: String = "expected_output"
    val stratificationPropertiesPath: String = "strat_properties"

    def readFile(file: String): DataFrame =
      if (endsWith(file)) {
        Helpers.sparkSession
          .read.option("header", "true").csv(file)
      } else { throw new Exception("File input must be in csv format.") }

    def retrievePath(maps: util.List[util.Map[String, String]])(path: String): String =
      maps.get(ParsingUtils.filePaths).get(ParsingUtils.inputPath)
  }

  Given("a dataset and a stratification properties file containing sample size and start point parameter:$") { dataTable: DataTable =>
    val dataTableAsListOfMaps = dataTable.asMaps(classOf[String], classOf[String])
    val mapOfPaths = ParsingUtils.retrievePath(dataTableAsListOfMaps)
    val inputPath = mapOfPaths(ParsingUtils.inputPath)
    val outputPath = mapOfPaths(ParsingUtils.outputPath)
    val stratificationPropertiesPath = mapOfPaths(ParsingUtils.stratificationPropertiesPath)
    println(s"Given file paths for inputPath [$inputPath], outputPath [$outputPath] and stratificationPropertiesPath [$stratificationPropertiesPath]")

    inputData = ParsingUtils.readFile(inputPath)
    stratProps = ParsingUtils.readFile(stratificationPropertiesPath)
    inputData.show()
  }
}
