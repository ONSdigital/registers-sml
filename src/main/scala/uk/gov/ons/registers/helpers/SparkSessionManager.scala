package uk.gov.ons.registers.helpers

import org.apache.spark.sql.{DataFrame, SparkSession}


private[registers] object SparkSessionManager {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Registers Statistical Methods Library (SML)")
    .getOrCreate()


  def withSpark(doWithinSparkSession: SparkSession => DataFrame): DataFrame = {
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Registers Statistical Methods Library (SML)")
      .getOrCreate()
    val df = doWithinSparkSession(sparkSession)

    sparkSession.stop()
    df
  }
}
