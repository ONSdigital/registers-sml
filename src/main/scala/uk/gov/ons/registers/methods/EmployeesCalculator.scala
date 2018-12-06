package uk.gov.ons.registers.methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait EmployeesCalculator {
  val imputed = "imp_empees"
  val employees = "empees"

  def calculateEmployees(empDF: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val calculatedEmployeesDF = getGroupedByEmployment(empDF)
    calculatedEmployeesDF
  }

  def getGroupedByEmployment(empDF: DataFrame, empTableName: String = "EMPLOYEES")(implicit spark: SparkSession): DataFrame = {
    val filEmpDF = empDF.select(ern, payeEmployees, imputed)
    filEmpDF.createOrReplaceTempView(empTableName)
    val flatEmpDataSql = generateCalculateEmpSql(empTableName)
    spark.sql(flatEmpDataSql).select(ern, employees)
  }

  def generateCalculateEmpSql(empTableName: String = "EMPLOYEES") =
    s"""
       SELECT $empTableName.*,
           CAST(
          ((CASE WHEN $empTableName.$payeEmployees IS NULL AND $empTableName.$imputed IS NULL THEN 1
                 WHEN $empTableName.$payeEmployees IS NULL THEN $empTableName.$imputed
                 ELSE $empTableName.$payeEmployees END))
           AS INT) AS empees
           FROM $empTableName
     """.stripMargin
}
