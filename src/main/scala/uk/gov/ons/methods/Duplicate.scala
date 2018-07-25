package uk.gov.ons.methods

import uk.gov.ons.methods.impl.DuplicateImpl
import org.apache.spark.sql.DataFrame

class Duplicate  (val dfIn: DataFrame) {

  if (dfIn == null) throw new Exception("DataFrame cannot be null")

  import DuplicateImpl._

  val defaultCol = "DuplicateMarking"

  private def mandatoryArgCheck(arg1 : List[String], arg2 : List[String], arg3 : String) : Unit = {

    if ((arg1 == null) || (arg2 == null) || (arg3 == null)) throw new Exception("Missing mandatory argument")
  }

  def dm1(df: DataFrame, partCol: List[String], ordCol: List[String], new_col: String = defaultCol) : DataFrame = {

    mandatoryArgCheck(partCol, ordCol, new_col)

    val dF = if (df == null) dfIn else df

    dF.duplicateMarking(partCol, ordCol, new_col)
  }

}

object Duplicate {

  def duplicate(df: DataFrame) : Duplicate = new Duplicate(df)
}