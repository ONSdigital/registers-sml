package uk.gov.ons.registers.model.selectionstrata

import uk.gov.ons.api.java.methods.registers.annotation.Description

case class SelectionStrata(
  @Description(value = "inqueryCode", description = "numerical code to identify survey, set to 687") inqcode: Int,
  @Description(value = "cellNumber", description = "unique identifier for each strata") cell_no: Int,
  @Description(value = "cellDescription", description = "text label for cell (strata)") cell_desc: String,
  @Description(value = "selectionType", description = "selection type U (no action required), P (random sample based on prn and no_reqd), C (census - all units selected)") seltype: String,
  @Description(value = "lowerClassSIC07", description = "lower range of SIC07") lower_class: Int,
  @Description(value = "upperClassSIC07", description = "upper range of SIC07") upper_class: Int,
  @Description(value = "lowerSizePayeEmployee", description = "lower range of size band: defined as paye_employees for test") lower_size: Long,
  @Description(value = "upperSizePayeEmployee", description = "upper range of size band: defined as paye_employees for test") upper_size: Long,
  @Description(value = "prnStartPoint", description = "starting point for random selection based on order of PRN") prn_start: BigDecimal,
  @Description(value = "numberRequired", description = "number required, only populated for 'P' type cells") no_reqd: Int
)
