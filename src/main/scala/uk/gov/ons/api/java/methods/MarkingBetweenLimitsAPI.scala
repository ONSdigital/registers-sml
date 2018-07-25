package uk.gov.ons.api.java.methods


import java.util

import org.apache.spark.sql.{Dataset, Row}
import uk.gov.ons.methods.MarkingBetweenLimits

import scala.collection.JavaConversions._

class MarkingBetweenLimitsAPI[K](mBL: MarkingBetweenLimits) {
  /** This function will call the function defined in methods.MarkingBetweenLimits.  It acts as a gateway between the
    * Java and the scala.
    * Any translations from Java Types to Scala types should be done here.
    *
    * @author kayleigh.bellis@ext.ons.gov.uk
    * @version 1.0
    * @param df               - The input DataFrame for the function to be applied to
    * @param valueColumnName  String - Name of the Column that will be divided by its previous
    * @param upperLimit       Any - The upper bound of what the ratio can be to be flagged 1
    * @param lowerLimit       Any - The lower bound of what the ratio can be to be flagged 1
    * @param partitionColumns List[String] - A list of column names that will be partition on when getting the
    *                         previous value
    * @param orderColumns     List[String] - A list of column names that will be order by when getting the
    *                         previous value
    * @param newColumnName    String - Name of the new Columns
    * @return DataFrame
    */

  def markingBetweenLimits(df: Dataset[Row], valueColumnName: String, upperLimit: Any, lowerLimit: Any,
                           partitionColumns: util.ArrayList[String], orderColumns: util.ArrayList[String],
                           newColumnName: String): Dataset[Row] = {
    mBL.markingBetweenLimitsMethod(df, valueColumnName, upperLimit, lowerLimit,
      partitionColumns.toList, orderColumns.toList, newColumnName)
  }

}

object MarkingBetweenLimitsAPI {
  def markingBetweenLimits(df: Dataset[Row]): MarkingBetweenLimitsAPI[MarkingBetweenLimits
    ] = {
    new MarkingBetweenLimitsAPI(MarkingBetweenLimits.markingBetweenLimits(df))
  }
}