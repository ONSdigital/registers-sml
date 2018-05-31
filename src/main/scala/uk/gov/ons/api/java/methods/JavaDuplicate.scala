package uk.gov.ons.api.java.methods

import java.util
import scala.collection.JavaConversions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import uk.gov.ons.methods.Duplicate

/**
  *
  * Java wrapper class used to access duplicate function
  *
  */
class JavaDuplicate[K](dm: Duplicate) {

  /** Java wrapper class used to access scala duplicate function
    *
    * author ian.edward@ext.ons.gov.uk
    * @param df            - Input DataFrame
    * @param partCol      - Column(s) to partition on
    * @param ordCol       - Column(s) to order on
    * @param new_col      - New column name
    * @return
    */
  def dm1(df: Dataset[Row], partCol: util.ArrayList[String], ordCol: util.ArrayList[String],
          new_col: String): DataFrame = {
    dm.dm1(df, partCol.toList, ordCol.toList, new_col)
  }

}
object JavaDuplicate{

  def duplicate(df: Dataset[Row]) : JavaDuplicate[Duplicate] = {
    new JavaDuplicate(Duplicate.duplicate(df))
  }
}