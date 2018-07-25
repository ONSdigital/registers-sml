package uk.gov.ons.api.java.methods

import java.util
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import scala.collection.JavaConversions._

import uk.gov.ons.methods.FirstReturn

/**
  * This is an interface between Scala first return and Java first return.
  * @param frstRtn
  * @tparam K
  */
class FirstReturnAPI[K](frstRtn: FirstReturn) {
  /**
    * This method is to flag up the first occurrence with flage value 1 and also large first occurrence with flage value 2.
    * @param df A DataFrame
    * @param partitionColumns A array of Strings - column(s) names.
    * @param orderColumns A array of Strings - column(s) names.
    * @param newColName A string - new column name.
    * @param thresholdPercentage A double - The first occurrence above this threshold should be flagged.
    * @param whichItemFr A string - which item first occurrence have to be marked for example here it is turnover.
    * @return Spark DataFrame with a newly added marker column
    */
  def firstReturn1(df: DataFrame, partitionColumns: util.ArrayList[String]
                   , orderColumns: util.ArrayList[String], newColName: String
                   , thresholdPercentage: Double, whichItemFr: String): DataFrame = {
    frstRtn.firstReturn1(df: DataFrame, partitionColumns.toList
      , orderColumns.toList, newColName
      , thresholdPercentage, whichItemFr)
  }
}

/**
  *
  */
object FirstReturnAPI {
  def firstReturn(df: Dataset[Row]): FirstReturnAPI[FirstReturn] = {
    new FirstReturnAPI(FirstReturn.firstReturn(df))
  }
}