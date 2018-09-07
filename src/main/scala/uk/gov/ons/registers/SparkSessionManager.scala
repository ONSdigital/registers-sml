package uk.gov.ons.registers

import org.apache.spark.sql.{DataFrame, SparkSession}

@deprecated
private[registers] object SparkSessionManager {
  private val SparkAppName = "Registers Statistical Methods Library (SML)"

  @deprecated
  implicit val sparkSession: SparkSession = SparkSession
    .builder()
    .appName(name = SparkAppName)
    .getOrCreate()

  @deprecated
  def withFunction(method: => DataFrame): DataFrame =
    SparkSession.getActiveSession.map { activeSession =>
      val resultDF = try method
      catch {
        case ex: Exception =>
          throw new Exception(s"Failed to construct DataFrame when running method with error", ex)
      }
      finally
        if (activeSession.sparkContext.appName == SparkAppName) {
          Patch.log(msg = s"Stopping active session [${activeSession.sparkContext.appName}] started on " +
            s"[${activeSession.sparkContext.startTime}] in thread.")
          activeSession.close
        }
      resultDF
    }.fold(throw new Exception("Failed to retrieve session instance from thread"))(identity)
}
