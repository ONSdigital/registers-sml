package uk.gov.ons.api.java.methods

import java.util

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ons.methods.Melt

import scala.collection.JavaConversions._

/**
  * Java wrapper class used to access scala melt functions.
  */
class MeltAPI[K](trans: Melt) {

  /** Java wrapper for the scala melt1 function.
    *
    * @author stuart.russell@ext.ons.gov.uk
    * @param input      - The input DataFrame
    * @param id_vars    - Column(s) which are used as unique identifiers
    * @param value_vars - Column(s) which are being unpivoted
    * @param var_name   - The name of a new column, which holds all the value_vars names, defaulted to variable.
    * @param value_name - The name of a new column, which holds all the values of value_vars column(s)
    * @return
    */
  def melt1(input: DataFrame, id_vars: util.ArrayList[String], value_vars: util.ArrayList[String],
            var_name: String = "variable", value_name: String = "value"): DataFrame = {
    trans.melt1(input, id_vars.toSeq, value_vars.toSeq, var_name, value_name)
  }

}

object MeltAPI {
  def melt(df: Dataset[Row]): MeltAPI[Melt] = {
    new MeltAPI(Melt.melt(df))
  }
}