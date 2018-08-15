package uk.gov.ons.registers.stepdefs

import scala.collection.JavaConverters.asScalaBufferConverter

import uk.gov.ons.registers.support.AssertionHelpers.assertThrown
import uk.gov.ons.registers.utils.DataTableExportUtil.saveTableAsCsv
import uk.gov.ons.registers.support.TestFileEnvSetup.{createAPath, createTempDirectory}

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

  And("""a Strata(?:|s| of selection type (?:Prn-Sampling|Census)) from Stratification Properties file:$"""){ aPropertiesTable: DataTable =>
    stratificationPropsPath = saveTableAsCsv(
      dataTable = aPropertiesTable,
      prefix = "stratified_properties_"
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

  Then("""an exception in Scala is thrown for .+ not being found upon trying to (?:Sample|Stratify)$"""){ () =>
    assertThrown()
  }
}
