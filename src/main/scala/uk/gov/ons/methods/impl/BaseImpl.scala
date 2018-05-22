package uk.gov.ons.methods.impl

import org.apache.spark.sql.expressions.Window
import uk.gov.ons.methods.impl.ONSRuntimeException
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.{Column, DataFrame, functions => F}

import scala.util.Try

object BaseImpl {

  implicit class BaseMethodsImpl(df: DataFrame) {

    /**This function will check the names of columns in the DataFrame against a list of inputs
      * to check that any methods that require specific columns will not fail.
      * @author james.a.smith@ext.ons.gov.uk
      * @version 1.0
      * @param columns String* - Name of the new Column
      * @return DataFrame
      */
    def checkColNames(columns: String*): DataFrame = {

      val colsFound = columns.flatMap(col => Try(df(col)).toOption)

      val okToContinue = columns.size == colsFound.size

      if (!okToContinue) throw ONSRuntimeException("Missing Columns Detected") else df
    }

   /* def duplicate_marking(df: DataFrame, partitionColumns: List[Column]
                          , orderColumns: List[Column], new_col: String): DataFrame = {
      //Create Window
      val w = Window.partitionBy(partitionColumns: _*).orderBy(orderColumns: _*)
      //Apply duplicate marker by marking the first row in a service
      df.withColumn("rank", F.row_number().over(w))
        .withColumn(new_col, F.when(F.col("rank") === 1, 1).otherwise(0))
        .drop(F.col("rank"))
    }*/
  }
}
