package uk.gov.ons.methods.impl

import uk.gov.ons.methods.impl.BaseImpl.BaseMethodsImpl
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.collection.mutable.ListBuffer

// TODO: Update this Apportionment implicit method.

object ApportionmentImpl {

  implicit class ApportionmentImplMethods(df: DataFrame)  extends BaseMethodsImpl(df : DataFrame) {

    /**
      * The Apportionment method aims to redistribute a numerical column based on given reference columns and an
      * auxiliary variable. The column to be distributed must be mapped from one reference column to a second reference
      * column. This is to account for potential 'complex' relationships that arise between references.
      *
      * If the relationship is 'simple', i.e. 1-to-1, then all of the numerical value from the first reference is
      * allocated to the second reference.
      *
      * For 'complex' relationships where reference A is associated with more than 1 reference B, the distribution of
      * the numerical value is calculated based on relative proportion of the auxiliary variable across all records of
      * reference B.
      *
      * For 'complex' relationships where more than 1 reference A is associated with 1 reference B, then the
      * distribution is the total of all values from records of reference A.
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

      // Create empty ListBuffer
      val appColumnsList = appColumns
      var appColumnsB_pre = new ListBuffer[String]()

      // Append the column names for the apportioned values
      for(colName <- appColumnsList) appColumnsB_pre += (colName + "_apportioned")
      val appColumnsB = appColumnsB_pre.toList

      // Sum the aux column by dateColumn and aggColumnA
      val sumAux: DataFrame = df.groupBy(dateColumn, aggColumnA)
                                       .sum(auxColumn)

      // Rename the joining columns to prevent errors with spark 2.x
      val renamedColumns: DataFrame = sumAux.withColumnRenamed(dateColumn, dateColumn + "1")
                                            .withColumnRenamed(aggColumnA, aggColumnA + "1")
                                            .withColumnRenamed("sum(" + auxColumn + ")", "sum_aux")

      // Join the sumAux DataFrame back on to the deDuplicated dataframe
      var joined: DataFrame = renamedColumns.join(df,
        df(dateColumn) === renamedColumns(dateColumn + "1") &&
          df(aggColumnA) === renamedColumns(aggColumnA + "1"))
        .drop(dateColumn + "1", aggColumnA + "1")

      for ((appColumn, count) <- appColumnsList.zipWithIndex) {
        joined = joined.withColumn(appColumnsB(count), col(auxColumn) / col("sum_aux")
          * col(appColumn))
      }

      // To account for complex relationships, group by dateColumn and aggColumnB and sum the turnovers
      val complex: DataFrame = joined.groupBy(dateColumn, aggColumnB).sum(appColumnsB: _*)

      // The summed columns need to be renamed to remove the sum()
      // The first step is to get the schema
      val schemaCols: Array[String] = complex.columns

      // The schema list is then narrowed down to the columns that do not need renamed
      var listCols = new ListBuffer[String]()
      for(colName <- schemaCols if !colName.startsWith("sum")) listCols += colName

      // The original column list added on to the new column list
      val newColList: List[String] = listCols.toList ::: appColumnsB

      // This list can then be applied to the DataFrame to rename all of the columns
      val columnsRenamed: DataFrame = complex.toDF(newColList: _*)

      // This DataFrame is then joined back on to the original non deDuplicated dataframe and working columns removed
      val apportioned: DataFrame = columnsRenamed.join(df, List(dateColumn, aggColumnB))

      apportioned
    }
  }
}
