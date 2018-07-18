package uk.gov.ons.registers.support.sample

import org.apache.spark.sql.Row


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

//  def apply(
//    ern: Option[String] = None, entref: String, name: String, tradingstyle: Option[String] = None, address1: String,
//    address2: Option[String] = None, address3: Option[String] = None, address4: Option[String] = None,
//    address5: Option[String] = None, postcode: String, legalstatus: String, sic07: String,
//    paye_empees: Option[String] = None, paye_jobs: Option[String] = None,ent_turnover: Option[String] = None,
//    std_turnover: Option[String] = None, grp_turnover: Option[String] = None, cntd_turnover: Option[String] = None,
//    app_turnover: Option[String] = None, prn: String, cell_no: String): Row = {
//    Row(
//      ern, entref, name, tradingstyle, address1, address2, address3, address4, address5, postcode, legalstatus, sic07,
//      paye_empees, paye_jobs, ent_turnover, std_turnover, grp_turnover, cntd_turnover, app_turnover, prn, cell_no
//    )
//  }

}
