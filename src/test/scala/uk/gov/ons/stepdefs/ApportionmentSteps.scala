package uk.gov.ons.stepdefs

import cucumber.api.scala.{EN, ScalaDsl}
import org.apache.spark.sql.DataFrame
import uk.gov.ons.methods.Apportionment
import scala.collection.JavaConverters._

class ApportionmentSteps extends ScalaDsl with EN {

  /**
    * Combines the first element (Java util.Map[String, String]) of two Java util.Lists and converts to a
    * single Scala Map[String, String].
    *
    * @param list1 : util.List[util.Map[String, String]\]
    * @param list2 : util.List[util.Map[String, String]\]
    */
  def convertJavaListsOfMaps(list1: java.util.List[java.util.Map[String, String]],
                             list2: java.util.List[java.util.Map[String, String]]): Map[String, String] = {
    List.concat(list1.get(0).asScala, list2.get(0).asScala).toMap
  }

  // Given("""^there are datasets at the locations:$""")
    // Do common step

  // Given("""^the user provides the parameters:$""")
    // Do common step

  Given("""^there is a simple relationship between two partitions$""") { () =>
    // Passing step, functionality tested by feature examples
    println("Given there is a simple relationship between two partitions")
  }

  Given("""^there is a complex relationship between two partitions$""") { () =>
    // Passing step, functionality tested by feature examples
    println("Given there is a complex relationship between two partitions")
  }

  When("""^the Scala Apportionment function is applied to the dataset$""") { () =>
    // Load parameters, create DataFrame and apply Apportionment function with parameters
    ContextApportionment.paramsMap = convertJavaListsOfMaps(ContextCommon.paths, ContextCommon.params)
    val inputDF = Helpers.sparkSession
                         .read
                         .json(ContextApportionment.paramsMap("input_data"))
    ContextApportionment.resultDF = Apportionment.apportionment(inputDF)
                                                 .app1(ContextApportionment.paramsMap("aux"),
                                                       ContextApportionment.paramsMap("date"),
                                                       ContextApportionment.paramsMap("aggregate A"),
                                                       ContextApportionment.paramsMap("aggregate B"),
                                                       ContextApportionment.paramsMap("apportion").split(",").toList)
    println("When the Scala Apportionment function is applied to the dataset")
  }

  Then(
    """^the Apportionment function will apportion the value to the output, matching an expected dataset at a location ([^"]*)$""".stripMargin) { (arg0: String) =>
    // Assert that the output DataFrame matches a DataFrame created from the expected data
    val expectedDF = Helpers.sparkSession
                            .read
                            .json(arg0)
    // Pull out columns from expected DataFrame to re-order columns in output DataFrame
    val selectList = expectedDF.columns

    assert(ContextApportionment.resultDF
                               .select(selectList.head, selectList.tail: _*)
                               .orderBy("vatref9", "turnover")
                               .collect()
      sameElements
           expectedDF.select(selectList.head, selectList.tail: _*)
                     .orderBy("vatref9", "turnover")
                     .collect())
    println("Then the Apportionment function will apportion the value to the output, matching an expected dataset " + "at a location " + arg0)
  }

  Then(
    """^the Apportionment function ensures that no turnover is lost in the output$""") { () =>
    // Assert that turnover is not lost for each aggregate B reference
    val target0 = ContextApportionment.paramsMap("apportion").split(",").toList(0)
    val aggB = ContextApportionment.paramsMap("aggregate B")

    for (refs <- ContextApportionment.resultDF.select(aggB).distinct().collect()) {
      val A = ContextApportionment.resultDF
                                  .filter(aggB + "=" + refs(0))
                                  .groupBy(aggB)
                                  .sum(target0)
                                  .select("sum(" + target0 + ")")
                                  .first()
      val B = ContextApportionment.resultDF
                                  .filter(aggB + "=" + refs(0))
                                  .select(target0 + "_apportioned")
                                  .first()
      assert(A == B)
    }
    println("Then the Apportionment function ensures that no turnover is lost in the output")
  }
}

object ContextApportionment {
  var resultDF: DataFrame = _
  var paramsMap: Map[String, String] = _
}
