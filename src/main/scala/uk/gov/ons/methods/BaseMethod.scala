package uk.gov.ons.methods

class BaseMethod {

  /**This function will confirm that all mandatory arguments have been passed to the function
    * @author james.a.smith@ext.ons.gov.uk
    * @version 1.0
    * @param args Any* - A list of any objects that are mandatory arguments
    * @return DataFrame
    */
  def mandatoryArgCheck(args : Any*) : Unit = {
    for (e <- args) if (e == null) throw new Exception("Missing Mandatory Argument")
  }
}
