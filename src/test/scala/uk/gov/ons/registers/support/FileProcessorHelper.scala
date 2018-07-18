package uk.gov.ons.registers.support

import java.io.File

import scala.io.Source.fromFile

object FileProcessorHelper {
  private val Delimiter = ","
  private val headerIndex = 1

//  private def splitRowStr(file: File): Array[String] = getFirstLineOfFile(file).split(Delimiter)

  private def getLines(file: File): Iterator[String] = fromFile(file).getLines

  private def getFirstLineOfFile(file: File): Iterator[String] = getLines(file).drop(headerIndex)

  def getFileHeader(file: File): String = getLines(file).next

  def getFileLength(file: File): Long = getLines(file).drop(headerIndex).size.toLong

  def getFirstLineAsColumnOfFile(file: File): Array[String] = getFirstLineOfFile(file).next.split(Delimiter)

//  def getNumberOfColumnsInFile(file: File): Int = splitRowStr(file).length

  def sampleSizeOfCellNumberInFile(file: File)(cellNumber: String): Long = getFirstLineOfFile(file).map(
    _.endsWith(Delimiter + cellNumber)
  ).count(_ equals true).toLong
}
