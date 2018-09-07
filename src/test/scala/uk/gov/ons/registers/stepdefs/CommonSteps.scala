package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.support.AssertionHelpers.assertThrown
import uk.gov.ons.registers.utils.DataTableTransformation.{RawDataTableList, createDataFrame}

import cucumber.api.scala.{EN, ScalaDsl}

class CommonSteps extends ScalaDsl with EN {

  And("""a Strata(?:|s| of selection type (?:Prn-Sampling|Census)) from Stratification Properties file:$"""){ aPropertiesTable: RawDataTableList =>
    stratificationPropsDF = createDataFrame(aPropertiesTable)
  }

  And("""a Stratification Properties file with an invalid field type:$"""){ anInvalidPropertiesTable: RawDataTableList =>
    stratificationPropsDF = createDataFrame(anInvalidPropertiesTable)
  }

  Then("""an exception in Scala is thrown for .+ due to a mismatch field type upon trying to (?:Sample|Stratify)$"""){ () =>
    assertThrown()
  }
}
