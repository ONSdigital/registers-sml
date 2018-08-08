package uk.gov.ons.registers.model

import scala.util.Try

sealed trait SelectionTypes

object SelectionTypes {
  case object Census extends SelectionTypes
  case object PrnSampling extends SelectionTypes
  case object Universal extends SelectionTypes

  object Initial {
    val census = "C"
    val prnSampling = "P"
    val universal = "U"
  }

  @deprecated
  private object Description {
    val census = "census"
    val prnSampling = "Sample"
    val universal = "Admin"
  }

  def fromString(str: String): Try[SelectionTypes] =
    Try(fromInitial(initialStr = str))

  def toInitial(selectionType: SelectionTypes): String =
    selectionType match {
      case Census => Initial.census
      case PrnSampling => Initial.prnSampling
      case Universal => Initial.universal
    }

  def fromInitial(initialStr: String): SelectionTypes =
    initialStr match {
      case Initial.census => Census
      case Initial.prnSampling => PrnSampling
      case Initial.universal => Universal
    }
}