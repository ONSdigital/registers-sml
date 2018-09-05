package uk.gov.ons.registers.methods

import global.AppParams
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.{DataFrame, SparkSession}

class PAYE(implicit activeSession: SparkSession) {

  val jobs = "paye_jobs"
  val employees = "paye_empees"

  def calculate(BIDF: DataFrame, payeDF: DataFrame, appConfs: AppParams): DataFrame = {
    val calculatedPayeDF = getGroupedByPayeRefs(BIDF, payeDF, "dec_jobs")
    //calculatedPayeDF.show
    calculatedPayeDF
  }

  def getGroupedByPayeRefs(BIDF: DataFrame, payeDF: DataFrame, quarter: String, luTableName: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA")(implicit activeSession: SparkSession) ={
    val flatUnitDf = BIDF.withColumn("payeref", explode_outer(BIDF.apply("PayeRefs")))

    flatUnitDf.createOrReplaceTempView(luTableName)
    payeDF.createOrReplaceTempView(payeDataTableName)

    implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
    val flatPayeDataSql = generateCalculateAvgSQL(luTableName, payeDataTableName)

    val sql = s"""
              SELECT SUM(AVG_CALCULATED.quarter_avg) AS $employees, CAST(SUM(AVG_CALCULATED.$quarter) AS int) AS $jobs, AVG_CALCULATED.ern
              FROM ($flatPayeDataSql) as AVG_CALCULATED
              GROUP BY AVG_CALCULATED.ern
            """.stripMargin
    spark.sql(sql)
  }

  def generateCalculateAvgSQL(luTablename: String = "LEGAL_UNITS", payeDataTableName: String = "PAYE_DATA") =
    s"""
      SELECT $luTablename.*, $payeDataTableName.mar_jobs, $payeDataTableName.june_jobs, $payeDataTableName.sept_jobs, $payeDataTableName.dec_jobs,
                CAST(
               (
                (CASE
                   WHEN $payeDataTableName.mar_jobs IS NULL
                   THEN 0
                   ELSE $payeDataTableName.mar_jobs
                 END +
                 CASE
                   WHEN $payeDataTableName.june_jobs IS NULL
                   THEN 0
                   ELSE $payeDataTableName.june_jobs
                 END +
                 CASE
                    WHEN $payeDataTableName.sept_jobs IS NULL
                    THEN 0
                    ELSE $payeDataTableName.sept_jobs
                 END +
                 CASE
                     WHEN $payeDataTableName.dec_jobs IS NULL
                     THEN 0
                     ELSE $payeDataTableName.dec_jobs
                 END)

                ) / (
                (CASE
                    WHEN $payeDataTableName.mar_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN $payeDataTableName.june_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                    WHEN $payeDataTableName.sept_jobs IS NULL
                    THEN 0
                    ELSE 1
                 END +
                 CASE
                   WHEN $payeDataTableName.dec_jobs IS NULL
                   THEN 0
                   ELSE 1
                 END)
                ) AS int) as quarter_avg

                FROM $luTablename
                LEFT JOIN $payeDataTableName ON $luTablename.payeref=$payeDataTableName.payeref
        """.stripMargin

  //    /**
  //      * calculates paye data (non-null data quarters count, total employee count, average) for 1 paye ref
  //      * */
  //
  //    def calculatePaye(unitsDF:DataFrame, payeDF:DataFrame)(implicit spark: SparkSession ) = {
  //      //printDF("unitsDF",unitsDF)
  //      val flatUnitDf = unitsDF.withColumn("payeref", explode_outer(unitsDF.apply("PayeRefs")))
  //
  //      val luTableName = "LEGAL_UNITS"
  //      val payeTableName = "PAYE_DATA"
  //
  //      flatUnitDf.createOrReplaceTempView(luTableName)
  //      payeDF.createOrReplaceTempView(payeTableName)
  //
  //      spark.sql(generateCalculateAvgSQL(luTableName,payeTableName))
  //
  //      /*  linkedPayes.show()
  //          linkedPayes.printSchema()*/
  //    }

}
object PAYE {
  def Paye(implicit sparkSession: SparkSession): PAYE =
    new PAYE
}