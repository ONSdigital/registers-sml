package uk.gov.ons.registers.methods

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.TransformFilesAndDataFrames.exportDfAsCsvOrError
import uk.gov.ons.registers.methods.impl.StratificationImpl
import uk.gov.ons.registers.model.stratification.Strata
import uk.gov.ons.registers.{SparkSessionManager, TransformFilesAndDataFrames}

class Stratification(inputPath: Path)(implicit activeSession: SparkSession) {

  import StratificationImpl._

  import activeSession.implicits._

  def stratify(stratificationPropsPath: Path, outputPath: Path): DataFrame = {
    val (frameDF, stratificationPropsDS) =
      TransformFilesAndDataFrames.validateAndConstructInputs[Strata](
        properties = inputPath, dataFile = stratificationPropsPath)
    TransformFilesAndDataFrames.validateOutputDirectory(outputPath)

    val arrayOfStratifiedFrames = stratificationPropsDS.rdd.collect.map{ row: Strata =>
      frameDF.stratify1(sic07LowerClass = row.lower_class, sic07UpperClass = row.upper_class,
        payeEmployeesLowerRange = row.lower_size, payeEmployeesUpperRange = row.upper_size, cellNo = row.cell_no)
    }
    val collectStrataFramesDF = TransformFilesAndDataFrames.transformToDataFrame(arrayOfDatasets = arrayOfStratifiedFrames)
    val strataFramesDF = frameDF.postStratification1(strataAllocatedDataFrame = collectStrataFramesDF)
    exportDfAsCsvOrError(dataFrame = strataFramesDF, path = outputPath)
    SparkSessionManager.stopSession()
    strataFramesDF
  }
}

object Stratification {
  def stratification(inputPath: Path)(implicit sparkSession: SparkSession): Stratification =
    new Stratification(inputPath)(sparkSession)
}
