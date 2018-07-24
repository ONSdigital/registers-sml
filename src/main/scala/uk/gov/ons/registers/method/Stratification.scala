package uk.gov.ons.registers.method

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import uk.gov.ons.registers.helpers.CSVProcessor.FilePath
import uk.gov.ons.registers.model.stratification.StratificationPropertiesRecord
import uk.gov.ons.registers.{SparkSessionManager, TransformFiles}

class Stratification(inputPath: FilePath) {

  import uk.gov.ons.registers.method.impl.StratificationImpl._

  def stratify(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {
    implicit val activeSession: SparkSession = SparkSessionManager.sparkSession

    val inputDataDF =  TransformFiles.readInputDataAsDF(inputPath)
    val stratificationPropsDS = TransformFiles.readStratificationPropsAsDS(stratificationPropsPath)

    val arrayOfStratification: Array[Dataset[Row]] = stratificationPropsDS.rdd.collect.map{ row: StratificationPropertiesRecord =>
      inputDataDF.stratify1(sic07LowerRange = row.lower_class, sic07UpperRange = row.upper_class,
        payeEmployeesLowerRange = row.lower_size, payeEmployeesUpperRange = row.upper_size, cellNo = row.cell_no)
    }
    val stratificationDF = TransformFiles.exportDatasetAsCSV(arrayOfDatasets = arrayOfStratification, outputPath = outputPath)
    stratificationDF
  }
}

object Stratification {
  def stratification(inputPath: FilePath): Stratification = new Stratification(inputPath)
}
