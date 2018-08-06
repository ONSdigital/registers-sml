package uk.gov.ons.registers.methods

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.methods.impl.StratificationImpl
import uk.gov.ons.registers.model.stratification.Strata
import uk.gov.ons.registers.{SparkSessionManager, TransformFiles}

class Stratification(inputPath: Path)(implicit activeSession: SparkSession) {

  import StratificationImpl._

  import activeSession.implicits._

  def stratify(stratificationPropsPath: Path, outputPath: Path): DataFrame = {
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
  def stratification(inputPath: Path)(implicit sparkSession: SparkSession): Stratification =
    new Stratification(inputPath)(sparkSession)
}
