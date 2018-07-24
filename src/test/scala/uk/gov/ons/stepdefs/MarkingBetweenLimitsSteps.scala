package uk.gov.ons.stepdefs

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ons.methods.MarkingBetweenLimits
import scala.collection.JavaConversions._


class MarkingBetweenLimitsSteps extends ScalaDsl with EN {

  Given("""^the user provides the function parameters:$""") { (X: DataTable) =>
    ContextCommon.param_list = X.asLists(classOf[String]).get(1).toSeq
  }

  // Given steps in common steps file
  // Common function to calling the markingBetweenLimitsMethod
  def callingMethod(): Unit = {
    val mBl = MarkingBetweenLimits.markingBetweenLimits(ContextCommon.input_data)
    val partCols: Seq[String] = ContextCommon.param_list(3).split(",").toList
    val orderCols: Seq[String] = ContextCommon.param_list(4).split(",").toList
    MarkingBetweenLimitsContext.resultDF = mBl.markingBetweenLimitsMethod(ContextCommon.input_data,
      ContextCommon.param_list(0), ContextCommon.param_list(1),
      ContextCommon.param_list(2), partCols, orderCols, ContextCommon.param_list(5))
  }

  //  Scenario : Ratio of Current and Previous value falls inbetween Limits
  When("""^the Scala value divided by the previous value falls in between the prescribed upper and lower values$""") {
    () =>
      callingMethod()
  }

  Then("""^the Scala record will be flagged as (\d+)$""") { (flag_value: Int) =>
    val expectedDf: Dataset[Row] = ContextCommon.expected_data
    assert(expectedDf.select("id", "date", "value", "marker").orderBy("id", "date").collect()
      sameElements
      MarkingBetweenLimitsContext.resultDF.select("id", "date", "value", "marker").orderBy("id", "date").collect())
  }

  // Scenario : Ratio of Current and Previous value is outside of Limits
  When("""^the Scala value divided by its previous is greater than the upper value or less than the lower limit$""") {
    () =>
      callingMethod()
  }


  // Scenario : No previous value
  When("""^the Scala current value is divided by the previous value, where previous value is null$""") {
    () =>
      callingMethod()
  }

  // Scenario : No current value
  When("""^there is no current value to perform the Scala calculation$""") { () =>
    callingMethod()
  }

  // Scenario : Previous value is zero
  When("""^the Scala current value is divided by the previous value, where previous value is zero$""") { () =>
    callingMethod()
  }
}

object MarkingBetweenLimitsContext {
  var resultDF: DataFrame = _
}
