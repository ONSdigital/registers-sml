package uk.gov.ons.stepdefs

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import uk.gov.ons.methods.Melt
import scala.collection.JavaConversions._

object ContextMelt {
  var input_data: DataFrame = _
  var id_vars: Seq[String] = _
  var value_vars: Seq[String] = _
  var var_name: String = _
  var value_name: String = _
}

class MeltSteps extends ScalaDsl with EN  {
  Given("""the user provides the melt function parameters:$""") { (x: DataTable) =>
    val inputList = x.asLists(classOf[String])
    ContextCommon.param_list = inputList.get(1).toSeq
    ContextMelt.input_data = ContextCommon.input_data
    ContextMelt.id_vars = ContextCommon.param_list.head.split(",")
    ContextMelt.value_vars = ContextCommon.param_list(1).split(",")
    ContextMelt.var_name = ContextCommon.param_list(2)
    ContextMelt.value_name = ContextCommon.param_list(3)
  }

  Given("""the Scala Melt function is applied""") { () =>
    if (ContextMelt.var_name == "null" && ContextMelt.value_name == "null") {
      ContextCommon.output_data = Melt.melt(ContextMelt.input_data)
        .melt1(ContextMelt.input_data, ContextMelt.id_vars, ContextMelt.value_vars)
    }
    else if (ContextMelt.var_name != "null" && ContextMelt.value_name != "null") {
      ContextCommon.output_data = Melt.melt(ContextMelt.input_data)
        .melt1(ContextMelt.input_data, ContextMelt.id_vars, ContextMelt.value_vars,
          ContextMelt.var_name, ContextMelt.value_name)
    }
    else if (ContextMelt.var_name != "null" && ContextMelt.value_name == "null") {
      ContextCommon.output_data = Melt.melt(ContextMelt.input_data)
        .melt1(ContextMelt.input_data, ContextMelt.id_vars, ContextMelt.value_vars,
          ContextMelt.var_name)
    }
    else if (ContextMelt.var_name == "null" && ContextMelt.value_name != "null") {
      ContextCommon.output_data = Melt.melt(ContextMelt.input_data)
        .melt1(ContextMelt.input_data, ContextMelt.id_vars, ContextMelt.value_vars,
          value_name = ContextMelt.value_name)
    }
  }

  Then("""the DataFrame will be unpivoted correctly""") { () =>
    if (ContextMelt.var_name == "null") {ContextMelt.var_name = "variable"}
    if (ContextMelt.value_name == "null") {ContextMelt.value_name = "value"}

    assert(ContextCommon.output_data.select("identifier", "date",
      ContextMelt.var_name, ContextMelt.value_name)
      .orderBy("identifier", "date", ContextMelt.value_name, ContextMelt.var_name).collect()
      sameElements ContextCommon.expected_data.select("identifier", "date",
      ContextMelt.var_name, ContextMelt.value_name)
      .orderBy("identifier", "date", ContextMelt.value_name, ContextMelt.var_name).collect())

    println("Input Data")
    println("Expected Output")
    ContextCommon.expected_data.show()
    println("Scala Melt output")
    ContextCommon.output_data.show()
  }
}
