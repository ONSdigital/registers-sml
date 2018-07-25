package uk.gov.ons.stepdefs

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, functions => F}
import uk.gov.ons.methods.FirstReturn

import scala.collection.JavaConversions._

class FirstReturnStep_S extends ScalaDsl with EN {

  Given(
    """^the user provides column names for the parameters partition,order,new column,which items first return and threshold percentage:$""".stripMargin) { (x: DataTable) =>
    var inputList = x.asLists(classOf[String])
    FirstReturnContext.param_list = inputList.get(1).toSeq
    FirstReturnContext.partition_cols = FirstReturnContext.param_list.head.split(",")
    FirstReturnContext.partition_list = FirstReturnContext.partition_cols.toList
    FirstReturnContext.order_cols = FirstReturnContext.param_list(1).split(",")
    FirstReturnContext.order_list = FirstReturnContext.order_cols.toList
    FirstReturnContext.new_cols_name = FirstReturnContext.param_list(2)
    FirstReturnContext.which_item_fr = FirstReturnContext.param_list(3)
    FirstReturnContext.threshold_percentage = FirstReturnContext.param_list(4).toDouble
  }

  When("""^we apply the Scala first returns function to identify first return and large first return$""") { () =>
    FirstReturnContext.frActualDf = FirstReturn.firstReturn(ContextCommon.input_data).firstReturn1(
      ContextCommon.input_data, FirstReturnContext.partition_list, FirstReturnContext.order_list,
      FirstReturnContext.new_cols_name, FirstReturnContext.threshold_percentage, FirstReturnContext.which_item_fr
    ).select("id", "period", "turnover", "turnover_fr_flag").orderBy("period", "id", "turnover_fr_flag")
    println("Identify Large FirstReturn Actual Dataframe ::: ")
    FirstReturnContext.frActualDf.show(70)
  }

  Then(
    """^the Scala first return should be marked as "([^"]*)",large first returns as "([^"]*)",otherwise "([^"]*)" as in data from the file location expected_data_path$""".stripMargin) { (x: String, y: String, z: String) =>
    println("Identify Large FirstReturn Expected Dataframe ::: ")
    ContextCommon.expected_data = ContextCommon.expected_data.select(F.col("id"), F.col("period"), F.col("turnover"),
      F.col("turnover_fr_flag").cast(DoubleType)).orderBy("period", "id", "turnover_fr_flag")
    ContextCommon.expected_data.show(70)
    assert(FirstReturnContext.frActualDf.collect() sameElements ContextCommon.expected_data.collect())
  }

  Given("""^the user provides the partition,order and new column name:$""") { (x: DataTable) =>
    var inputList = x.asLists(classOf[String])
    ContextCommon.param_list = inputList.get(1).toSeq
    ContextCommon.input_data = ContextCommon.input_data
    FirstReturnContext.partition_cols = ContextCommon.param_list.head.split(",")
    FirstReturnContext.partition_list = FirstReturnContext.partition_cols.toList
    FirstReturnContext.order_cols = ContextCommon.param_list(1).split(",")
    FirstReturnContext.order_list = FirstReturnContext.order_cols.toList
    FirstReturnContext.new_cols_name = ContextCommon.param_list(2)
  }

  When("""^we apply the Scala identify first returns function$""") { () =>
    import uk.gov.ons.methods.impl.FirstReturnImpl._
    FirstReturnContext.frActualDf = ContextCommon.input_data.flagupFirstReturn(
      FirstReturnContext.partition_list, FirstReturnContext.order_list,
      FirstReturnContext.new_cols_name
    ).select("id", "period", "turnover", "turnover_fr_flag").orderBy("period", "id", "turnover_fr_flag")
    println("FirstReturn Actual Dataframe ::: ")
    FirstReturnContext.frActualDf.show(70)
  }
  Then(
    """^the Scala first return should be marked as "([^"]*)",otherwise "([^"]*)" as in data from the file location expected_data_path$""".stripMargin) { (x: String, y: String) =>
    println("FirstReturn Expected Dataframe ::: ")
    ContextCommon.expected_data.orderBy("period", "id", "turnover_fr_flag").show(70)
    assert(FirstReturnContext.frActualDf.collect() sameElements ContextCommon.expected_data.select(F.col("id"),
      F.col("period"), F.col("turnover"), F.col("turnover_fr_flag").cast(IntegerType)).orderBy("period", "id",
      "turnover_fr_flag").collect())
  }

  Given(
    """^the user provides the threshold percentage,which item's firstreturn to be flagged up, partition and new columns:$""".stripMargin) { (x: DataTable) =>
    var inputList = x.asLists(classOf[String])
    FirstReturnContext.param_list = inputList.get(1).toSeq
    FirstReturnContext.partition_cols = FirstReturnContext.param_list.head.split(",")
    FirstReturnContext.partition_list = FirstReturnContext.partition_cols.toList
    FirstReturnContext.which_item_fr = FirstReturnContext.param_list(1)
    FirstReturnContext.new_cols_name = FirstReturnContext.param_list(2)
    FirstReturnContext.threshold_percentage = FirstReturnContext.param_list(3).toDouble
  }

  When("""^we apply the Scala large first returns function$""") { () =>
    import uk.gov.ons.methods.impl.FirstReturnImpl._
    FirstReturnContext.frActualDf = ContextCommon.input_data.flagupTopPercentageFr(
      FirstReturnContext.partition_list, FirstReturnContext.which_item_fr,
      FirstReturnContext.new_cols_name, FirstReturnContext.threshold_percentage
    ).select("id", "period", "turnover", "turnover_fr_flag").orderBy("period", "id", "turnover_fr_flag")
    println("Identify Large FirstReturn Actual Dataframe ::: ")
    FirstReturnContext.frActualDf.show(70)
  }
  Then(
    """^the Scala large first return should be marked as "([^"]*)" as in in data from the file location expected_data_path$""".stripMargin) { (arg0: String) =>
    println("Identify Large FirstReturn Expected Dataframe ::: ")
    ContextCommon.expected_data.show(70)
    ContextCommon.expected_data = ContextCommon.expected_data.select(F.col("id"), F.col("period"), F.col("turnover"),
                                                                     F.col("turnover_fr_flag").cast(IntegerType))
                                                                     .orderBy("period", "id", "turnover_fr_flag")
    assert(FirstReturnContext.frActualDf.collect() sameElements ContextCommon.expected_data.collect())
  }

}

object FirstReturnContext {
  var order_cols: Seq[String] = _
  var frActualDf: DataFrame = _
  var partition_cols: Seq[String] = _
  var param_list: Seq[String] = _
  var partition_list: List[String] = _
  var order_list: List[String] = _
  var new_cols_name: String = _
  var threshold_percentage: Double = _
  var which_item_fr: String = _
}