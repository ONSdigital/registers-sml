package uk.gov.ons.methods

import org.apache.spark.sql.DataFrame

class Melt(val dfIn: DataFrame) extends BaseMethod {
  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import uk.gov.ons.methods.impl.MeltImpl._

  /** Scala function that calls version 1 of the melt method.
    *
    * @author stuart.russell@ext.ons.gov.uk
    * @param input      DataFrame        - The input DataFrame
    * @param id_vars    Seq[String]    - Column(s) which are used as unique identifiers
    * @param value_vars Seq[String] - Column(s) which are being unpivoted
    * @param var_name   String        - The name of a new column, which holds all
    *                   the value_vars names, defaulted to variable.
    * @param value_name String      - The name of a new column, which holds all the
    *                   values of value_vars column(s), defaulted to value.@param input
    * @return DataFrame
    */
  def melt1(input: DataFrame = dfIn, id_vars: Seq[String], value_vars: Seq[String],
            var_name: String = "variable", value_name: String = "value"): DataFrame = {
    mandatoryArgCheck(id_vars, value_vars)
    input.melt1(id_vars, value_vars, var_name, value_name)
  }

}

object Melt {
  def melt(df: DataFrame): Melt = new Melt(df)
}
