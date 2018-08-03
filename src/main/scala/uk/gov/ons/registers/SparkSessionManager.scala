package uk.gov.ons.registers

import org.apache.spark.sql.SparkSession

private[registers] object SparkSessionManager {
  private val sparkAppName = "Registers Statistical Methods Library (SML)"

  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(name = sparkAppName)
    .getOrCreate()

  def stopSession(): Unit =
    SparkSession.getActiveSession.foreach{ activeSession =>
      if (activeSession.sparkContext.appName == sparkAppName) {
        // TODO - ADD logger that SparkSession is being closed
        println(s"[INFO] Stopping active session [${activeSession.sparkContext.appName}] started on " +
          s"[${activeSession.sparkContext.startTime}] in thread.")
        activeSession.close
      }
    }
}
