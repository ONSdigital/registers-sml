package uk.gov.ons.registers

import org.apache.spark.sql.{DataFrame, SparkSession}


private[registers] object SparkSessionManager {
  @deprecated("Migrate to withSpark in future commit(s)")
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Registers Statistical Methods Library (SML)")
    .getOrCreate()


  def withSpark(doWithinSparkSession: SparkSession => DataFrame) = {
    implicit val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("Registers Statistical Methods Library (SML)")
      .getOrCreate()
    val df = doWithinSparkSession(sparkSession)

    sparkSession.stop()
    df
  }
}
