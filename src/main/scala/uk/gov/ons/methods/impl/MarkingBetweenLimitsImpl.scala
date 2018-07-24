package uk.gov.ons.methods.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, when}
import org.apache.spark.sql.{Column, Dataset, Row}
import uk.gov.ons.methods.impl.BaseImpl.BaseMethodsImpl

object MarkingBetweenLimitsImpl {

  implicit class MarkingBetweenLimitsMethodsImpl(df: Dataset[Row]) extends BaseMethodsImpl(df: Dataset[Row]) {
    /** This function marks a record with a one or a zero depending on whether the ratio of the given value/previous
      * value is in between the limits or not, respectively.
      *
      * It does this first lagging back on the value column, to get its previous one. When lagging  back this is first
      * partitioned by the columns specified then ordered, in order to be able to get the previous ones. This created as
      * an extra column.
      *
      * Then it divides the current value by its previous to get the ratio of the two, this is also created as an extra
      * column and the previous column is dropped since its no longer needed.
      *
      * Then the ratio is compared to the upper and lower limits, if its between these limits then the record is marked
      * as a one. If its outside these  limits then its marked as a zero, these markers are stored in a new column on
      * the DataFrame which name is given as a parameter.
      *
      * {{{ lowerLimit < value/previousValue > upperLimit  }}}
      *
      * @author kayleigh.bellis@ext.ons.gov.uk
      * @version 1.0
      * @param valueColumnName  String - Name of the Column that will be divided by its previous
      * @param upperLimit       Any - The upper bound of what the ratio can be to be flagged 1
      * @param lowerLimit       Any - The lower bound of what the ratio can be to be flagged 1
      * @param partitionColumns List[String] - A list of column names that will be partition on when getting the
      *                         previous value
      * @param orderColumns     List[String] - A list of column names that will be order by when getting the
      *                         previous value
      * @param newColumnName    String - Name of the new Column
      * @return Dataset[Row]
      */
    def markingBetweenLimitsMethod(valueColumnName: String, upperLimit: Any, lowerLimit: Any,
                                   partitionColumns: Seq[String], orderColumns: Seq[String],
                                   newColumnName: String): Dataset[Row] = {
      //Naming variables
      val laggedColumn: String = "previous" + valueColumnName
      val ratioColumn: String = "ratio" + valueColumnName
      // Re-mapping
      val pColumns: Seq[Column] = partitionColumns.map(s => col(s))
      val oColumns: Seq[Column] = orderColumns.map(s => col(s))
      // Create window
      val w = Window.partitionBy(pColumns: _*).orderBy(oColumns: _*)
      // Create the previous column
      val previousDf: Dataset[Row] = df.withColumn(laggedColumn, lag(valueColumnName, 1, null).over(w))
      // Create the ratio column, of the value divide by the previous  one
      val ratioDf: Dataset[Row] = previousDf.withColumn(ratioColumn,
        col(valueColumnName) / col(laggedColumn)).drop(col(laggedColumn))
      // Creating the marker column based on whether the ratio lies between the limits
      val markerDf: Dataset[Row] = ratioDf.withColumn(newColumnName,
        when(col(ratioColumn).between(lowerLimit, upperLimit), 1).otherwise(0)).drop(ratioColumn)
      // returning the Dataset[Row]
      markerDf
    }
  }

}
