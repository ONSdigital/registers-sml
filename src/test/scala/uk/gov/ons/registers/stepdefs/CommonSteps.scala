package uk.gov.ons.registers.stepdefs

import java.util

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

  private object ParsingUtils {
    private val FilePathsIndex = 0
    val InputPath: String = "data_input_path"
    val OutputPath: String = "expected_output"
    val StratificationPropertiesPath: String = "strat_properties"

    def retrievePath(maps: util.List[util.Map[String, String]])(path: String): String =
      maps.get(FilePathsIndex).get(path)
  }

  Given("a frame and a stratification properties file paths:$") { dataTable: DataTable =>
    import ParsingUtils._

    val dataTableAsListOfMaps = dataTable.asMaps(classOf[String], classOf[String])
    val mapOfPaths = retrievePath(dataTableAsListOfMaps) _
    inputPath = mapOfPaths(InputPath)
    outputPath = mapOfPaths(OutputPath)
    stratificationPropertiesPath = mapOfPaths(StratificationPropertiesPath)
    println(s"Given file paths for inputPath [$inputPath], outputPath [$outputPath] and stratificationPropertiesPath [$stratificationPropertiesPath]")
  }
}
