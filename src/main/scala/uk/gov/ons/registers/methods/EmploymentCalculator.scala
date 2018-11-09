package uk.gov.ons.registers.methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait EmploymentCalculator {

  val imputed = "imp_empees"
  val work_prop = "working_prop"

  def calculateEmployment(empDF: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val calculatedEmployeesDF = getGroupedByEmployment(empDF)
    calculatedEmployeesDF
  }

  def getGroupedByEmployment(empDF: DataFrame, empTableName: String = "EMPLOYMENT")(implicit spark: SparkSession): DataFrame = {
    val filEmpDF = empDF.select(ern, payeEmployees, imputed, work_prop)
    filEmpDF.createOrReplaceTempView(empTableName)
    val flatEmpDataSql = generateCalculateEmpSql(empTableName)
    spark.sql(flatEmpDataSql).select(ern, employment)
  }

  def generateCalculateEmpSql(empTableName: String = "EMPLOYMENT") =
    s"""
       SELECT $empTableName.*,
           CAST(
          ((CASE WHEN $empTableName.$payeEmployees IS NULL AND $empTableName.$imputed IS NULL THEN 1
                 WHEN $empTableName.$payeEmployees IS NULL THEN $empTableName.$imputed + $empTableName.$work_prop
                 ELSE $empTableName.$payeEmployees + $empTableName.$work_prop END))
           AS INT) AS employment
           FROM $empTableName
     """.stripMargin

}