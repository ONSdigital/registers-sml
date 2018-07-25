package uk.gov.ons.stepdefs
import uk.gov.ons.methods.Duplicate
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import cucumber.api.DataTable
import scala.collection.JavaConversions._

object ContextDuplicate {

  var partition_cols : Seq[String] = _
  var order_cols : Seq[String] = _
  var input_data: DataFrame = _
  var expected_data: DataFrame = _
  var partition_list: List[String] = _
  var order_list: List[String] = _
}
//noinspection ScalaStyle
class DuplicateMarkerSteps extends ScalaDsl with EN {


  Given("""^the user provides the partition and order columns:$"""){ (x:DataTable) =>

    val inputList = x.asLists(classOf[String])
    ContextCommon.param_list = inputList.get(1).toSeq
    ContextDuplicate.input_data = ContextCommon.input_data
    ContextDuplicate.partition_cols = ContextCommon.param_list.head.split(",")
    ContextDuplicate.partition_list = ContextDuplicate.partition_cols.toList
    ContextDuplicate.order_cols = ContextCommon.param_list(1).split(",")
    ContextDuplicate.order_list = ContextDuplicate.order_cols.toList

  }

  When("""^the Scala Duplicate Marker function is applied$""") { () =>

    ContextCommon.output_data = Duplicate.duplicate(ContextDuplicate.input_data)
      .dm1(ContextCommon.input_data, ContextDuplicate.partition_list,
        ContextDuplicate.order_list, DuplicateContext.new_column_name)
    println("Scala Duplicate Marker DataFrame")
    println("Scala Duplicate DataFrame")
    ContextCommon.output_data.show()

  }

  Then("""^record with highest order column is marked with one to match expected data$""") { () =>

    println("Expected DataFrame")
    ContextCommon.expected_data.show()
    assert(ContextCommon.expected_data.select("id", "num", "order", "Duplicate Marker").collect()
      sameElements ContextCommon.output_data.select("id", "num", "order", "Duplicate Marker").collect())
  }

}

object DuplicateContext {
  var new_column_name: String = "Duplicate Marker"
}
