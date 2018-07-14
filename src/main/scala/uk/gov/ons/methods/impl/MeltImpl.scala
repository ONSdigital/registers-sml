package uk.gov.ons.methods.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}

object MeltImpl {

  implicit class MeltMethodsImpl(df: DataFrame) {

    /** This method will take a sequence of column names (strings) and unpivots
      * them into two columns, the '''var_name''' and its values.
      * It does this by creating an array of structs through the value_var, this produces the following format (if two
      * end parameters are kept default)
      *
      *                                {{{ array<struct<variable : str, value: ... >> }}}
      *
      * This array is then added to the dataframe under the column name '''_vars_and_vals'''
      *
      * A list of columns is then created, which holds only the '''id_vars''', the '''var_name''' and the
      * '''value_name'''
      *
      * EG, '''var_name''' and '''value_name''' defaults.
      *
      *                 {{{  List(id_var[0], id_var[1], ...., id_var[n], _vars_and_vals['variable'] as 'variable',
      *                 vars_and_vals['value'] as 'value') }}}
      *
      *
      * When the dataframe is selected with this list, it puts the variable part of the array as the variable column
      * and it puts the value part of the array as the value column. Unpivoting the dataframe.
      *
      * @author stuart.russell@ext.ons.gov.uk
      * @version 1.0
      * @param id_vars Seq[String]    - Column(s) which are used as unique identifiers
      * @param value_vars Seq[String] - Column(s) which are being unpivoted
      * @param var_name String        - The name of a new column, which holds all the value_vars names, defaulted to
      *                                 variable.
      * @param value_name String      - The name of a new column, which holds all the values of value_vars column(s),
      *                                 defaulted to value.
      * @return DataFrame
      */
    def melt1(id_vars: Seq[String], value_vars: Seq[String], var_name: String ="variable",
              value_name: String ="value"): DataFrame = {

      // Start of melting the dataframe
      // Create array<struct<variable : str, value: ... >>
      val _vars_and_vals = F.array((for (c <- value_vars) yield {
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
      }): _*)

      // Add to the DataFrame and explode
      val _tmp: DataFrame = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
      val cols = id_vars.map(F.col _) ++ {
        for (x <- List(var_name, value_name)) yield {
          F.col("_vars_and_vals")(x).alias(x)
        }
      }
      _tmp.select(cols:_*)
    }

  }

}
