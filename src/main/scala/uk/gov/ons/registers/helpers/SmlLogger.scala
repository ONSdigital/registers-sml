package uk.gov.ons.registers.helpers

import org.apache.spark.sql.DataFrame

trait SmlLogger {
  def logPartitionInfo(df:DataFrame, line:Int, message:String = "") = {
    val partitions = df.rdd.getNumPartitions
    println(message)
    println(s"Sample:$line Number of partitions: $partitions")
  }
}
