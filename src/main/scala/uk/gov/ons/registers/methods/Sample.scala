package uk.gov.ons.registers.methods

import javax.inject.Singleton
import org.apache.spark.sql._

@Singleton
class Sample(implicit spark: SparkSession) {

  def create(stratifiedFrameDf: DataFrame, stratificationPropsDf: DataFrame): DataFrame = {
    val records = "records"

    stratifiedFrameDf.createOrReplaceTempView(records)
    val propsList: Array[Row] = stratificationPropsDf.filter(stratificationPropsDf("seltype") === "P" || stratificationPropsDf("seltype") === "C").collect()//createOrReplaceTempView(props)
    val emptyRecordDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stratifiedFrameDf.schema)

    def selectSampleSql(cellNum:String,seltype:String, resultsNum:String, prnStart:String) = {

      val sampleSize = if(seltype=="P") s"LIMIT $resultsNum" else ""
      val prnCondition = if(seltype=="P") s"(CAST(prn AS FLOAT) >= CAST('$prnStart' AS FLOAT))" else ""
      val orderByPrn = if(seltype=="P") "ORDER BY prn" else ""

      s"""
         SELECT * from $records
         WHERE cell_no='$cellNum' AND $prnCondition
         $orderByPrn
         $sampleSize

       """.stripMargin


    }

    propsList.foldRight(emptyRecordDF){(propRow,agg) => {
      val sampleDF = spark.sql(selectSampleSql(
                                                propRow.getAs[String]("cell_no"),
                                                propRow.getAs[String]("seltype"),
                                                propRow.getAs[String]("no_reqd"),
                                                propRow.getAs[String]("prn_start")
                                                ))
      agg.union(sampleDF)}}


  }
}

object Sample {
  def sample(implicit sparkSession: SparkSession): Sample =
    new Sample
}

