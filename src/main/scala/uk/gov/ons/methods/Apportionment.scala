package uk.gov.ons.methods

import uk.gov.ons.methods.impl.ApportionmentImpl._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class Apportionment (val df: DataFrame) {

  if (df == null) throw new Exception("DataFrame cannot be null")

  val defaultCol = "AppResult"

  private def mandatoryArgCheck(arg1: String, arg2: String, arg3: String, arg4: String): Unit = {
    if ((arg1 == null) || (arg2 == null) || (arg3 == null) || (arg4 == null)) throw new Exception("Missing mandatory argument")
  }

  /**
    * Scala method that calls the Apportionment implicit method.
    *
    * @author danny.fiske@ons.gov.uk
    * @version 1.0
    *
    * @param auxColumn    String - column used as a weighting.
    * @param dateColumn   String - column used to differentiate records of the same reference.
    * @param aggColumnA   String - column containing initial reference.
    * @param aggColumnB   String - column containing reference to apportion to.
    * @param appColumns   List[String] - column(s) to be redistributed.
    * @return             Dataset[Row]
    */
  def app1(auxColumn: String, dateColumn: String, aggColumnA:
  String, aggColumnB: String, appColumns: List[String]): DataFrame = {

    mandatoryArgCheck(auxColumn, dateColumn, aggColumnA, aggColumnB)

    val dF = if (df == null) df else df

    dF.checkColNames(auxColumn, dateColumn, aggColumnA, aggColumnB)
      .app1(auxColumn, dateColumn, aggColumnA, aggColumnB, appColumns)
  }
}

object Apportionment {

  def apportionment(df: DataFrame) : Apportionment = new Apportionment(df)
}
