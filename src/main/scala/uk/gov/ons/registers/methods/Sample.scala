package uk.gov.ons.registers.methods

import javax.inject.Singleton
import org.apache.spark.sql._
import org.apache.spark.sql.functions._




@Singleton
class Sample(implicit spark: SparkSession) {

  def logPartitionInfo(df:DataFrame, line:Int, message:String = "") = {
    val partitions = df.rdd.getNumPartitions
    println(message)
    println(s"Sample:$line Number of partitions: $partitions")
  }

  def validateProps(cellNum:String,prnStart:String) = {
    val validateCellNumberError = if(!cellNum.matches("^\\d+$")) s"cell number in not a number: $cellNum" else ""
    val validatePrnStart = if(!prnStart.matches("^0.\\d+$")) s"prnStart in not a valid prn: $prnStart" else ""
    val errs = List(validateCellNumberError, validatePrnStart).filter(!_.trim.isEmpty)
    if(errs.nonEmpty) {
      val errMessage = errs.mkString("; ")
      throw new IllegalArgumentException(s"Problem parsing inputs: $errMessage")
    }
  }

  def create(stratifiedFrameDf: DataFrame, stratificationPropsDf: DataFrame): DataFrame = {
    val records = "records"

    val defaultPartitions = stratifiedFrameDf.rdd.getNumPartitions

    logPartitionInfo(stratifiedFrameDf,34)

    stratifiedFrameDf.createOrReplaceTempView(records)
    val propsList: Array[Row] = stratificationPropsDf.filter(stratificationPropsDf("seltype") === "P" || stratificationPropsDf("seltype") === "C").collect()//createOrReplaceTempView(props)
    val emptyRecordDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], stratifiedFrameDf.schema)

    logPartitionInfo(stratifiedFrameDf,40)

    def selectBasicSampleSql(cellNum:String,seltype:String, resultsNum:String, prnStart:String) = {

      val sampleSize = if(seltype=="P") s"LIMIT $resultsNum" else ""
      val prnCondition = if(seltype=="P") s" AND (CAST(prn AS FLOAT) >= CAST('$prnStart' AS FLOAT))" else ""
      val orderByPrn = if(seltype=="P") "ORDER BY prn" else ""

      s"""
         SELECT * from $records
         WHERE cell_no='$cellNum' $prnCondition
         $orderByPrn
         $sampleSize

       """.stripMargin
    }

    def selectSampleSql(cellNum:String,seltype:String, resultsNum:String, prnStart:String, remainingSampleCount:Long) = {

     s"""
         SELECT * FROM $records
         where WHERE cell_no='$cellNum'
         ORDER BY prn ASC
         LIMIT ${remainingSampleCount.toString}
       """.stripMargin
    }


    propsList.foldRight(emptyRecordDF){(propRow,agg) => {

      val cellNu = propRow.getAs[String]("cell_no")
      val seltype = propRow.getAs[String]("seltype")
      val sampleSize = propRow.getAs[String]("no_reqd")
      val startingPrn = propRow.getAs[String]("prn_start")
      validateProps(cellNu,startingPrn)

      val primaryQuery = selectBasicSampleSql(cellNu, seltype, sampleSize, startingPrn)
      val basicSampleDFPQ = spark.sql(primaryQuery)

      logPartitionInfo(basicSampleDFPQ,79)

      val basicSampleDF = basicSampleDFPQ//.coalesce(defaultPartitions)

      val sampleDF = if(seltype=="P") {
        val remainingSample = sampleSize.toLong - basicSampleDF.count()
        if(remainingSample>0) {
            val secondaryQuery = selectSampleSql(cellNu, seltype, sampleSize, startingPrn, remainingSample)
            val secondaryResDF = spark.sql(secondaryQuery)
            logPartitionInfo(secondaryResDF,88)
            (secondaryResDF.union(basicSampleDF)).distinct.orderBy(desc("prn"))
        }else basicSampleDF
      } else basicSampleDF.distinct
      logPartitionInfo(basicSampleDF,92)
      val df = agg.union(sampleDF)
      logPartitionInfo(basicSampleDF,94,"Aggregated sample DF")
      df
    }}
  }
}

object Sample {
  def sample(implicit sparkSession: SparkSession): Sample =
    new Sample
}

