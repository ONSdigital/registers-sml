package uk.gov.ons.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import cucumber.api.DataTable
import org.apache.spark.sql.DataFrame

object ContextCommon {
  var input_data: DataFrame = _
  var expected_data: DataFrame = _
  var param_list: Seq[String] = _
  var output_data: DataFrame = _
}

class CommonSteps extends ScalaDsl with EN {
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
