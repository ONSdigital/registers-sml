package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.support.DataTableExportUtil.saveTableAsCsv
import uk.gov.ons.registers.support.TestFileEnvSetup.createAPath

import cucumber.api.DataTable
import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

  And("""a Strata .*from Stratification Properties file:$"""){ aPropertiesTable: DataTable =>
    stratificationPropsPath = saveTableAsCsv(
      dataTable = aPropertiesTable,
      prefix = "stratified_properties"
    )
  }

  And("""a Stratification Properties file that does not exist$"""){ () =>
    stratificationPropsPath = createAPath(pathStr = "invalid_stratification_props_path")
  }
}
