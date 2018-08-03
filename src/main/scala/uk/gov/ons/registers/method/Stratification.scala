package uk.gov.ons.registers.method

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.{SparkSessionManager, TransformFiles}
import uk.gov.ons.registers.helpers.CSVProcessor.FilePath
import uk.gov.ons.registers.model.stratification.Strata

class Stratification(inputPath: FilePath)(implicit activeSession: SparkSession) {

  import uk.gov.ons.registers.method.impl.StratificationImpl._
  import activeSession.implicits._

  def stratify(stratificationPropsPath: FilePath, outputPath: FilePath): DataFrame = {
    val (stratifiedFrameDF, stratificationPropsDS) =
      TransformFiles.validateAndConstructInputs[Strata](
        properties = inputPath, dataFile = stratificationPropsPath)

    val arrayOfStratification = stratificationPropsDS.rdd.collect.map{ row: Strata =>
      stratifiedFrameDF.stratify1(sic07LowerClass = row.lower_class, sic07UpperClass = row.upper_class,
        payeEmployeesLowerRange = row.lower_size, payeEmployeesUpperRange = row.upper_size, cellNo = row.cell_no)
    }
    val stratificationDF = TransformFiles.exportDatasetAsCSV(arrayOfDatasets = arrayOfStratification, outputPath = outputPath)
    SparkSessionManager.stopSession()
    stratificationDF
  }
}

object Stratification {
  def stratification(inputPath: FilePath)(implicit sparkSession: SparkSession): Stratification =
    new Stratification(inputPath)(sparkSession)
}
