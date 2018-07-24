package uk.gov.ons.methods

import org.apache.spark.sql.DataFrame

/**
  *This class is to identify the first occurrence and also large first occurrence. .
  * @param dfIn
  */
class FirstReturn(val dfIn: DataFrame) {
  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import uk.gov.ons.methods.impl.FirstReturnImpl._

  val defaultCol = "firstReturnFlag"

  /**
    *This method applies the two methods- first flagupFirstReturn,next flagupTopPercentageFr on input DataFrame.
    *
    * @param df A DataFrame
    * @param partitionColumns A list of Strings - column(s) names.
    * @param orderColumns A list of Strings - column(s) names.
    * @param newColName A string - new column name
    * @param thresholdPercentage A double - The  first occurrence above this threshold should be flagged.
    * @param whichItemFr A string - which item first occurrence have to be marked for example here it is turnover
    * @return Spark DataFrame with a newly added marker column
    */
  def firstReturn1(df: DataFrame, partitionColumns: List[String]
                   , orderColumns: List[String], newColName: String
                   , thresholdPercentage: Double, whichItemFr: String): DataFrame = {
    val dF: DataFrame = if (df == null) dfIn else df

    df.checkColNames(Seq(partitionColumns, orderColumns).flatten)
      .flagupFirstReturn(partitionColumns, orderColumns, newColName)
      .flagupTopPercentageFr(orderColumns, whichItemFr, newColName, thresholdPercentage)
  }
}

/**
  * This is an entry point to the FirstReturn impletentations
  */
object FirstReturn {

  def firstReturn(df: DataFrame): FirstReturn = {
    new FirstReturn(df)
  }

}
