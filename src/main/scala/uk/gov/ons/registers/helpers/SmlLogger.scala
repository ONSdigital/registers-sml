package uk.gov.ons.registers.helpers

import org.apache.spark.sql.DataFrame

trait SmlLogger {
  def logPartitionInfo(df:DataFrame, line:Int, classname:String, message:String = "") = {
    val partitions = df.rdd.getNumPartitions
    println(message)
    println(s"$classname:$line: Number of partitions: $partitions")
  }
}
