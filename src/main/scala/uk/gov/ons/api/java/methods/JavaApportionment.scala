package uk.gov.ons.api.java.methods

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ons.methods.Apportionment
import scala.collection.JavaConverters._

class JavaApportionment[K] (app: Apportionment) {

  /**
    * Scala method that calls the Apportionment implicit method.
    * Method acts as a bridge between Java and Scala
    *
    * @author danny.fiske@ons.gov.uk
    * @version 1.0
    *
    * @param auxColumn    String - column used as a weighting.
    * @param dateColumn   String - column used to differentiate records of the same reference.
    * @param aggColumnA   String - column containing initial reference.
    * @param aggColumnB   String - column containing reference to apportion to.
    * @param appColumns   String - column(s) to be redistributed.
    * @return             Dataset[Row]
    */
  def app1(auxColumn: String, dateColumn: String,
           aggColumnA: String, aggColumnB: String, appColumns: java.util.ArrayList[String]): DataFrame = {

    app.app1(auxColumn, dateColumn,
             aggColumnA, aggColumnB, appColumns.asScala.toList)
  }
}

object JavaApportionment {

  def apportionment(df: Dataset[Row]): JavaApportionment[Apportionment] = {

    new JavaApportionment(Apportionment.apportionment(df))
  }
}
