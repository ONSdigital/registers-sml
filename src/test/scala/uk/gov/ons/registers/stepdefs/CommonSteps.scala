package uk.gov.ons.registers.stepdefs

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import scala.collection.JavaConversions._

import uk.gov.ons.registers.support.TestFileEnvSetup

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

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

  private def saveTableAsCsv(dataTable: DataTable, prefix: String): Path = {
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

  Given("""a [^\s]+ Frame:$"""){ aStratifiedFrameTable: DataTable =>
    stratifiedFramePath = saveTableAsCsv(
      dataTable = aStratifiedFrameTable,
      prefix = "stratified_frame")
  }

  And("""a Strata of selection type [^\s]+ from Stratification Properties file:$"""){ aPropertiesTable: DataTable =>
    stratificationPropsPath = saveTableAsCsv(
      dataTable = aPropertiesTable,
      prefix = "stratified_properties"
    )
  }
}
