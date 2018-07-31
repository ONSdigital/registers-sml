package uk.gov.ons.registers

import org.apache.spark.sql.SparkSession

private[registers] object SparkSessionManager {
  private val sparkAppName = "Registers Statistical Methods Library (SML)"

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(name = sparkAppName)
    .getOrCreate()

  // TODO - ADD logger that SparkSession is being closed
  def stopSession(): Unit =
    SparkSession.getActiveSession.foreach{ activeSession =>
      if (activeSession.sparkContext.appName == sparkAppName) activeSession.close
    }
}
