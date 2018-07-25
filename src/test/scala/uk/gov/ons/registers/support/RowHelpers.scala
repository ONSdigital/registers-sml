package uk.gov.ons.registers.support

import uk.gov.ons.registers.support.sample.SampleEnterpriseRow.empty

object RowHelpers {

  private def valueOrNull(str: String): String =
    Option(str.isEmpty).fold[String](empty)(_ => str)

  def getField[A](map: Map[String, A])(fieldStr: String): A =
    map.getOrElse(key = valueOrNull(fieldStr), default = throw new NoSuchElementException(s"Could not find field " +
      s"[$fieldStr] in map [$map]"))

}
