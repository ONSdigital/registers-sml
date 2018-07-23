package uk.gov.ons.methods.impl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, functions => F}
import uk.gov.ons.methods.impl.BaseImpl.BaseMethodsImpl

/**
  * This is an implementation of identifying the first return of large return.
  */
object FirstReturnImpl {

  /**
    *The implicit class allows us to call the method from the dataframe itself and
    * apply it to that dataframe.
    *
    * @param df
    */
  implicit class FirstReturnMethodImp(df: DataFrame) extends BaseMethodsImpl(df: DataFrame) {
    /**
      * Takes a spark dataframe, applies the window specification based on partition and orderby columns.
      * It then adds a new column with value 1 ,if it is the first occurrence otherwise zero.
      *
      * @param partitionColumns A list of Strings - column(s) names.
      * @param orderColumns A list of Strings - column(s) names.
      * @param newColName A string - new column name
      * @return Spark DataFrame with a newly added marker column
      */
    def flagupFirstReturn(partitionColumns: List[String]
                          , orderColumns: List[String], newColName: String): DataFrame = {
      val aCols: List[Column] = partitionColumns.map(s => F.col(s))
      val bCols: List[Column] = orderColumns.map(s => F.col(s))
      val frFlaggedup: DataFrame = duplicate_marking(df, aCols, bCols, newColName)
      frFlaggedup
    }

    /**
      * This function takes a dataframe and splits it into 2, one containing first returns
      * the other with non-first returns.The large first returs are then flagged up with value 2,
      * then it combined with the original non first returns dataframe to produce a consolidated dataframe.
      *
      * @param partitionColumns A list of Strings - column(s) names.
      * @param whichItemFr A string - which item first occurrence have to be marked for example here it is turnover
      * @param newColName A string - new column name
      * @param thresholdPercentage A double - The first occurrence above this threshold should be flagged.
      * @return Spark DataFrame
      */
    def flagupTopPercentageFr(partitionColumns: List[String]
                              , whichItemFr: String, newColName: String
                              , thresholdPercentage: Double
                             ): DataFrame = {
      val aCols: List[Column] = partitionColumns.map(s => F.col(s))
      val bCols: Column = F.col(whichItemFr)
      val firstReturnDf = df.where(df(newColName) === 1)
      val nonFirstReturnDf = df.where(df(newColName) === 0)
      val colNameList: List[String] = nonFirstReturnDf.schema.fieldNames.toList
      val largeFrWindow = Window.partitionBy(aCols: _*).orderBy(bCols.desc)
      val largeFr_ntile = firstReturnDf.withColumn("lfr_flag", F.ntile(100).over(largeFrWindow))
      //val largeFr_ntile1 = firstReturnDf.withColumn("lfr_flag1", F.rank().over(largeFrWindow))
      val largeFrDf = largeFr_ntile.withColumn("lfr_flag", F.when(largeFr_ntile("lfr_flag") <= thresholdPercentage, 1).otherwise(0))
      val combinedDf = largeFrDf.withColumn(newColName, largeFrDf(newColName) + largeFrDf("lfr_flag"))
        .drop("lfr_flag").select(colNameList.map(F.col): _*)
      val final_df = nonFirstReturnDf.union(combinedDf)
      final_df
    }
  }

}
