package uk.gov.ons.registers.support

import java.io.File

import scala.io.Source.fromFile

object FileProcessorHelper {
  val Delimiter = ","
  private val headerIndex = 1

  private def getLines(file: File): Iterator[String] = fromFile(file).getLines

  def getLinesWithoutHeader(file: File): Iterator[String] = getLines(file).drop(headerIndex)

  def getFileHeader(file: File): Array[String] = getLines(file).next.split(Delimiter)

  @deprecated
  def getFileLength(file: File): Long = getLines(file).drop(headerIndex).size.toLong

  @deprecated
  def getFirstLineOfFile(file: File): String = getLinesWithoutHeader(file).next

  @deprecated
  def getFirstLineAsColumnOfFile(file: File): Array[String] = getFirstLineOfFile(file).split(Delimiter)

  @deprecated
  def sampleSizeOfCellNumberInFile(file: File)(cellNumber: String): Long = getLinesWithoutHeader(file).map(
    _.endsWith(Delimiter + cellNumber)
  ).count(_ equals true).toLong

  @deprecated
  def createMapFromLine(schema: Array[String])(linesAsArray: Array[String]): Map[String, String] =
    (schema zip linesAsArray).toMap

  @deprecated
  def lineWithHeaderAsMap(sampleCsvFile: File, getFileLine: File => Array[String] = getFirstLineAsColumnOfFile): Map[String, String] =
    createMapFromLine(schema = getFileHeader(sampleCsvFile))(linesAsArray = getFileLine(sampleCsvFile))

  def lineAsListOfFields(file: File): List[List[String]] =
    getLines(file = file).map(_.split(Delimiter).toList).toList

}
