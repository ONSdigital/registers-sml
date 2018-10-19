package uk.gov.ons.registers.methods

import javax.inject.Singleton

import org.apache.spark.sql.{DataFrame, SparkSession}

import uk.gov.ons.registers.TransformDataFrames.{fromArrayDataFrame, validateAndParseInputs}
import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting.checkUnitForMandatoryFields
import uk.gov.ons.registers.model.selectionstrata.SelectionStrata
import uk.gov.ons.registers.methods.impl.StratificationImpl._

@Singleton
trait Stratification {

  def stratify(inputDf: DataFrame, stratificationPropsDf: DataFrame, unitSpecDF: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val bounds = unitSpecDF.head().getString(1)
    val (frameDF, stratificationPropsDS) =
      validateAndParseInputs(propertiesDf = stratificationPropsDf, unitDf = inputDf, bounds,
        validateFields = checkUnitForMandatoryFields)
    /**
      * NOTE - the driver is solely aware of the type T in Dataset[T] and cannot be inferred by worker nodes.
      *        Collect forces the transformation to be returned to the node allowing the proceeding step to incur
      *        as desired
      */
    val arrayOfStratifiedFrames = stratificationPropsDS.collect.map{ selectionStrata: SelectionStrata =>
      val strata = frameDF.stratify1(sic07LowerClass = selectionStrata.lower_class, sic07UpperClass = selectionStrata.upper_class,
        payeEmployeesLowerRange = selectionStrata.lower_size, payeEmployeesUpperRange = selectionStrata.upper_size,
        cellNo = selectionStrata.cell_no, bounds)
      frameDF.postPayeEmployeeNullDenotation1(strata = strata, sic07LowerClass = selectionStrata.lower_class,
        sic07UpperClass = selectionStrata.upper_class, bounds)
    }
    val collectStrataFramesDF = fromArrayDataFrame(arrayOfDatasets = arrayOfStratifiedFrames)
    frameDF.postStratification1(strataAllocatedDataFrame = collectStrataFramesDF)
  }
}



