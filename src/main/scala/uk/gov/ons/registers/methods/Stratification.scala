package uk.gov.ons.registers.methods

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.TransformFilesAndDataFrames.exportDfAsCsvOrError
import uk.gov.ons.registers.methods.impl.StratificationImpl
import uk.gov.ons.registers.model.stratification.SelectionStrata
import uk.gov.ons.registers.{SparkSessionManager, TransformFilesAndDataFrames}

class Stratification(inputPath: Path)(implicit activeSession: SparkSession) {

  import StratificationImpl._

  import activeSession.implicits._

  def stratify(stratificationPropsPath: Path, outputPath: Path): DataFrame = {
    SparkSessionManager.terminateSession {
      val (frameDF, stratificationPropsDS) =
        TransformFilesAndDataFrames.validateAndConstructInputs[SelectionStrata](
          properties = inputPath, dataFile = stratificationPropsPath)
      TransformFilesAndDataFrames.validateOutputDirectory(outputPath)
      /**
        * NOTE - the driver is solely aware of the type T in Dataset[T] and cannot be inferred by worker nodes.
        *        Collect forces the transformation to be returned to the node allowing the proceeding step to incur
        *        as desired
        */
      val arrayOfStratifiedFrames = stratificationPropsDS.collect.map{ selectionStrata: SelectionStrata =>
        frameDF.stratify1(sic07LowerClass = selectionStrata.lower_class, sic07UpperClass = selectionStrata.upper_class,
          payeEmployeesLowerRange = selectionStrata.lower_size, payeEmployeesUpperRange = selectionStrata.upper_size,
          cellNo = selectionStrata.cell_no)
      }
      val collectStrataFramesDF = TransformFilesAndDataFrames.transformToDataFrame(arrayOfDatasets = arrayOfStratifiedFrames)
      val strataFramesDF = frameDF.postStratification1(strataAllocatedDataFrame = collectStrataFramesDF)
      exportDfAsCsvOrError(dataFrame = strataFramesDF, path = outputPath)
      strataFramesDF
    }
  }
}

object Stratification {
  def stratification(inputPath: Path)(implicit sparkSession: SparkSession): Stratification =
    new Stratification(inputPath)(sparkSession)
}
