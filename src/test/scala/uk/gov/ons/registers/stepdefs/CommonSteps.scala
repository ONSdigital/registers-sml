package uk.gov.ons.registers.stepdefs

import scala.collection.JavaConverters.asScalaBufferConverter

import uk.gov.ons.registers.support.DataTableExportUtil.saveTableAsCsv
import uk.gov.ons.registers.support.TestFileEnvSetup.{createAPath, createTempDirectory}

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

  And("""a (?:Strata|Strata of selection type Prn-Sampling|Strata of selection type Census|Stratas) from Stratification Properties file:$"""){ aPropertiesTable: DataTable =>
    stratificationPropsPath = saveTableAsCsv(
      dataTable = aPropertiesTable,
      prefix = "stratified_properties"
    )
  }

  And("""a Stratification Properties file that does not exist$"""){ () =>
    stratificationPropsPath = createAPath(pathStr = "invalid_stratification_props_path")
  }

  And("""an output path to store the result is given:$"""){ aPathTable: DataTable =>
    val outputPathPrefix = aPathTable.asList(classOf[String]).asScala.toList.head
    outputPath = createTempDirectory(prefix = outputPathPrefix)
  }

  And("""an output path to store the result is not given"""){ () =>
    outputPath = createAPath(pathStr = "")

  }
}
