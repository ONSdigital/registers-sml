package uk.gov.ons.registers.stepdefs

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.Path

import scala.collection.JavaConversions._

import uk.gov.ons.registers.support.TestFileEnvSetup
import uk.gov.ons.registers.support.TestFileEnvSetup.createAPath

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

  Given("""a Stratified Frame:$"""){ aFrameTable: DataTable =>
    stratifiedFramePath = saveTableAsCsv(
      dataTable = aFrameTable,
      prefix = "stratified_frame")
  }

  Given("""a Frame:$"""){ aFrameTable: DataTable =>
    framePath = saveTableAsCsv(
      dataTable = aFrameTable,
      prefix = "frame")
  }

  And("""a Strata of selection type [^\s]+ from Stratification Properties file:$"""){ aPropertiesTable: DataTable =>
    stratificationPropsPath = saveTableAsCsv(
      dataTable = aPropertiesTable,
      prefix = "stratified_properties"
    )
  }

  And("""a Stratification Properties file that does not exist$"""){ () =>
    stratificationPropsPath = createAPath(pathStr = "invalid_stratfification_props_path")
  }



//  And("""a Stratification Properties file that does not exist$"""){ () =>
//    println("0-0-0-0-0-0=0=-0-0-=0-=0=0-=0=-0")
//  }
//
//  Given("""a * does not exist$"""){ () =>
//    println("0-0-0-0-0-0=0=-0-0-=0-=0=0-=0=-0")
//  }
}
