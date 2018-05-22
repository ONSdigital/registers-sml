package uk.gov.ons.stepdefs

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._

class CommonSteps extends ScalaDsl with EN {

  Given("""^the user provides the parameters:$""") { (arg0: DataTable) =>
    // Receives Cucumber DataTable and converts to List[Map[String, String]]
    ContextCommon.params = arg0.asMaps(classOf[String], classOf[String])
    println("Given the user provides the parameters: " + ContextCommon.params)

  }

  Given("""^there are datasets at the locations:$""") { (arg0: DataTable) =>
    // Receives Cucumber DataTable and converts to List[Map[String, String]]
    ContextCommon.paths = arg0.asMaps(classOf[String], classOf[String])
    println("Given there are datasets at the locations: " + ContextCommon.paths)
  }

  Given("""the user provides the file paths:$""") { (x: DataTable) =>
    val pathList = x.asLists(classOf[String])
    val inputPath: String = pathList.get(1).get(0)
    val expectedPath: String = pathList.get(1).get(1)

    if (inputPath.endsWith("csv")) {
      ContextCommon.input_data = Helpers.sparkSession
        .read.option("header", "true").csv(inputPath)
    } else if (inputPath.endsWith("json")) {
      ContextCommon.input_data = Helpers.sparkSession
        .read.json(inputPath)
    } else {
      throw new Exception("File input must be in JSON or csv format.")
    }
    if (expectedPath.endsWith("csv")) {
      ContextCommon.expected_data = Helpers.sparkSession
        .read.
        option("header", "true").csv(expectedPath)
    } else if (expectedPath.endsWith("json")) {
      ContextCommon.expected_data = Helpers.sparkSession
        .read.json(expectedPath)
    } else {
      throw new Exception("File input must be in JSON or csv format.")
    }
  }
}

// Object stores variables for later Scala/Java steps
object ContextCommon {
  var params: java.util.List[java.util.Map[String, String]] = _
  var paths: java.util.List[java.util.Map[String, String]] = _
  var input_data: DataFrame = _
  var expected_data: DataFrame = _
  var param_list: Seq[String] = _
  var output_data: DataFrame = _
}
