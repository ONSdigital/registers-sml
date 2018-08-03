package uk.gov.ons.registers.support.sample

import org.apache.spark.sql.Row

import uk.gov.ons.registers.support.RowHelpers


object SampleEnterpriseRow {

  val empty = null
  val ern = "ern"
  val entref = "entref"
  val name = "name"
  val tradingstyle = "tradingstyle"
  val address1 = "address1"
  val address2 = "address2"
  val address3 = "address3"
  val address4 = "address4"
  val address5 = "address5"
  val postcode = "postcode"
  val legalstatus = "legalstatus"
  val sic07 = "sic07"
  val paye_empees = "paye_empees"
  val paye_jobs = "paye_jobs"
  val ent_turnover = "ent_turnover"
  val std_turnover = "std_turnover"
  val grp_turnover = "grp_turnover"
  val cntd_turnover = "cntd_turnover"
  val app_turnover = "app_turnover"
  val prn = "prn"
  val cell_no = "cell_no"

  def apply(
             ern: String = empty, entref: String, name: String, tradingstyle: String = empty, address1: String,
             address2: String = empty, address3: String = empty, address4: String = empty, address5: String = empty,
             postcode: String, legalstatus: String, sic07: String, paye_empees: String = empty, paye_jobs: String = empty,
             ent_turnover: String = empty, std_turnover: String = empty, grp_turnover: String = empty,
             cntd_turnover: String = empty, app_turnover: String = empty, prn: String, cell_no: String): Row = {
    Row(
      ern, entref, name, tradingstyle, address1, address2, address3, address4, address5, postcode, legalstatus, sic07,
      paye_empees, paye_jobs, ent_turnover, std_turnover, grp_turnover, cntd_turnover, app_turnover, prn, cell_no
    )
  }

  def enterpriseRowFromMap(csvRowAsColumnsWithMap: Map[String, String]): Row = {
    val parseStrFieldAsRowField = RowHelpers.getField(csvRowAsColumnsWithMap) _
    SampleEnterpriseRow(
      ern =parseStrFieldAsRowField(ern), entref=parseStrFieldAsRowField(entref), name=parseStrFieldAsRowField(name),
      tradingstyle=parseStrFieldAsRowField(tradingstyle), address1=parseStrFieldAsRowField(address1),
      address2=parseStrFieldAsRowField(address2), address3=parseStrFieldAsRowField(address3),
      address4=parseStrFieldAsRowField(address4), address5=parseStrFieldAsRowField(address5),
      postcode=parseStrFieldAsRowField(postcode), legalstatus=parseStrFieldAsRowField(legalstatus),
      sic07=parseStrFieldAsRowField(sic07), paye_empees=parseStrFieldAsRowField(paye_empees),
      paye_jobs=parseStrFieldAsRowField(paye_jobs), ent_turnover=parseStrFieldAsRowField(ent_turnover),
      std_turnover= parseStrFieldAsRowField(std_turnover), grp_turnover=parseStrFieldAsRowField(grp_turnover),
      cntd_turnover=parseStrFieldAsRowField(cntd_turnover), app_turnover=parseStrFieldAsRowField(app_turnover),
      prn=parseStrFieldAsRowField(prn), cell_no=parseStrFieldAsRowField(cell_no)
    )
  }

}
