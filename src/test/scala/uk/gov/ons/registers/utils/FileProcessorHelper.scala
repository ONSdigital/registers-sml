package uk.gov.ons.registers.utils

import java.io.File

import scala.io.Source.fromFile

@deprecated
object FileProcessorHelper {
  val Delimiter = ","
  private val headerIndex = 1

  @deprecated
  private def getLines(file: File): Iterator[String] = fromFile(file).getLines

  @deprecated
  def getLinesWithoutHeader(file: File): Iterator[String] = getLines(file).drop(headerIndex)

  @deprecated
  def getFileHeader(file: File): Array[String] = getLines(file).next.split(Delimiter)

  @deprecated
  def lineAsListOfFields(file: File): List[List[String]] =
    getLines(file = file).map(_.split(Delimiter).toList).toList
}
