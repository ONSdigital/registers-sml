package uk.gov.ons.registers.utils

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import uk.gov.ons.registers.helpers.CSVProcessor.DefaultFileDelimiter
import uk.gov.ons.registers.support.TestFileEnvSetup

import cucumber.api.DataTable


object DataTableExportUtil {
  // Loan
  private def withWriter(file: File)(f: BufferedWriter => Unit): Unit = {
    val fileWriter = new FileWriter(file)
    try {
      val bufferedWriter = new BufferedWriter(fileWriter)
      try f(bufferedWriter)
      finally bufferedWriter.close()
    }
    finally fileWriter.close()
  }

  def saveTableAsCsv(dataTable: DataTable, prefix: String): Path = {
    val aListOfTableRows = dataTable.asLists(classOf[String])
    val testTempPath = TestFileEnvSetup.createTempFile(prefix)
    withWriter(file = testTempPath.toFile){ writer =>
      aListOfTableRows.asScala.foreach { row =>
        writer.append(row.asScala.mkString(DefaultFileDelimiter))
        writer.newLine()
      }
    }
    testTempPath
  }
}
