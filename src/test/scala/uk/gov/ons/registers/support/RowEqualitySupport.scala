package uk.gov.ons.registers.support

import org.apache.spark.sql.Row

// TODO - Use the actual Row.equals and use replace (e.g. "" => null)
object RowEqualitySupport {
  @deprecated
  def equals(actualRow: Row, expectedRow: Row): Boolean = {
    (0 until expectedRow.length).map(field =>
      if (actualRow.isNullAt(field) || actualRow.getAs[String](field).isEmpty)
        null == expectedRow.get(field)
      else actualRow.get(field) == expectedRow.get(field)
    ).forall(_ equals true)
  }

  @deprecated
  def replace(row: Row, pattern: String, newValue: String): Row =
    Row.fromSeq((0 until row.length).map{field =>
      if(row.getAs[String](field).isEmpty) null
      else row(field)
    })
}
