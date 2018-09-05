package uk.gov.ons.registers.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType}
import uk.gov.ons.registers.model.CommonFrameDataFields.{payeEmployees, prn, sic07}
import uk.gov.ons.registers.model.selectionstrata.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber
import uk.gov.ons.registers.model.CommonPayeFrameFields.{employees, jobs}

object CommonFrameAndPropertiesFieldsCasting {
  private val NullableValuesAllowed = 0

  def checkUnitForMandatoryFields(unitDF: DataFrame): DataFrame = {
    val castedUnitDf = unitDF
      .withColumn(colName = payeEmployees, unitDF.col(payeEmployees).cast(LongType))
      .withColumn(colName = sic07, unitDF.col(sic07).cast(IntegerType))
      .withColumn(colName = prn, unitDF.col(prn).cast(DataTypes.createDecimalType(precision, scale)))

    if (castedUnitDf.filter(castedUnitDf(payeEmployees).isNull || castedUnitDf(sic07).isNull ||
      castedUnitDf(prn).isNull).count > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check common mandatory fields [$payeEmployees, $sic07, $prn] are of expected type")
    else castedUnitDf
  }

  def checkStratifiedFrameForMandatoryFields(stratifiedDF: DataFrame): DataFrame = {
    val castedStratifiedDF = stratifiedDF
      .withColumn(colName = cellNumber, stratifiedDF.col(cellNumber).cast(IntegerType))
      .withColumn(colName = prn, stratifiedDF.col(prn).cast(DataTypes.createDecimalType(precision, scale)))

    if (castedStratifiedDF.filter(castedStratifiedDF(cellNumber).isNull ||
      castedStratifiedDF(prn).isNull).count > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check common mandatory fields [$cellNumber, $prn] are of expected type")
    else castedStratifiedDF
  }

  def checkPayeforMandatoryFields(PayeDF: DataFrame): DataFrame = {
    val castedPayeDF = PayeDF
      .withColumn(colName = employees, PayeDF.col(employees).cast(LongType))
      .withColumn(colName = jobs, PayeDF.col(jobs).cast(IntegerType))

    if (castedPayeDF.filter(castedPayeDF(employees).isNull || castedPayeDF(jobs).isNull).count() > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check mandatory fields [$employees, $jobs] are of expected type")
    else castedPayeDF
  }
}
