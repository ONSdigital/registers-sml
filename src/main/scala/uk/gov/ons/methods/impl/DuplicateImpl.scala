package uk.gov.ons.methods.impl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, when, row_number}
import BaseImpl.BaseMethodsImpl

object DuplicateImpl {
  implicit class DuplicateMethodsImpl(df: DataFrame) extends BaseMethodsImpl(df : DataFrame){

    /** This method will flag any duplicate records
      *
      * This method adds a column to a dataframe containing duplicate markers.
      * 0 = Duplicate
      * 1 = Not a Duplicate
      *
      * @author Tom Reilly
      * @version 1.0
      * @param partCol A list of the column(s) to check for duplicates within
      * @param ordCol A list of the column(s) To order by
      * @param new_col The name of the new column that will contain the markers
      * @return DataFrame
      */
    def duplicateMarking(partCol: List[String], ordCol: List[String],
                         new_col: String): DataFrame = {

      val pCols: List[Column] = partCol.map(s => col(s))
      val oCols: List[Column] = ordCol.map(s => col(s))
      // Create window
      val w = Window.partitionBy(pCols:_*).orderBy(oCols:_*)

      // Apply duplicate marker by marking the first row in a series
      df.withColumn("rank", row_number().over(w))
        .withColumn(new_col, when(col("rank") === 1, 1).otherwise(0))
        .drop(col("rank"))
    }

  }
}
