package uk.gov.ons.registers.support

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import scala.collection.JavaConversions._

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
    val testTempPath = TestFileEnvSetup.createTempFile(prefix = prefix)
    withWriter(file = testTempPath.toFile){ writer =>
      aListOfTableRows.foreach { row =>
        // TODO - move delimitor
        writer.append(row.mkString(","))
        writer.newLine()
      }
    }
    testTempPath
  }
}
