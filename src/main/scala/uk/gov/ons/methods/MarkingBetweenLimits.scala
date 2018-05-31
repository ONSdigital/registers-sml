package uk.gov.ons.methods

import org.apache.spark.sql.DataFrame

class MarkingBetweenLimits(val dfIn: DataFrame) extends BaseMethod {
  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import uk.gov.ons.methods.impl.MarkingBetweenLimitsImpl._

  /** Scala function that calls version 1 of the marking between limits method.
    *
    * @author kayleigh.bellis@ext.ons.gov.uk
    * @version 1.0
    * @param input            DataFrame - The input DataFrame for the function to be applied to
    * @param valueColumnName  String - Name of the Column that will be divided by its previous
    * @param upperLimit       Any - The upper bound of what the ratio can be to be flagged 1
    * @param lowerLimit       Any - The lower bound of what the ratio can be to be flagged 1
    * @param partitionColumns List[String] - A list of column names that will be partition on when getting the
    *                         previous value
    * @param orderColumns     List[String] - A list of column names that will be order by when getting the
    *                         previous value
    * @param newColumnName    String - Name of the new Column
    * @return DataFrame
    */
  def markingBetweenLimitsMethod(input: DataFrame = dfIn, valueColumnName: String, upperLimit: Any, lowerLimit: Any,
                              partitionColumns: Seq[String], orderColumns: Seq[String],
                              newColumnName: String): DataFrame = {
    mandatoryArgCheck(valueColumnName, upperLimit, lowerLimit, newColumnName,
      Seq(partitionColumns, orderColumns).flatten)
    input.markingBetweenLimitsMethod(valueColumnName, upperLimit, lowerLimit,
      partitionColumns, orderColumns, newColumnName)
  }
}

object MarkingBetweenLimits {
  def markingBetweenLimits(df: DataFrame): MarkingBetweenLimits = new MarkingBetweenLimits(df)
}
