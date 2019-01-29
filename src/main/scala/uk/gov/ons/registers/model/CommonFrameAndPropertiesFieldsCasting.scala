package uk.gov.ons.registers.model

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, IntegerType, LongType}
import uk.gov.ons.registers.model.CommonFrameDataFields.{employment, _}
import uk.gov.ons.registers.model.selectionstrata.PrnNumericalProperty.{precision, scale}
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields.cellNumber

object CommonFrameAndPropertiesFieldsCasting {
  private val NullableValuesAllowed = 0

  def checkUnitForMandatoryFields(unitDF: DataFrame, bounds: String)(implicit sparkSession: SparkSession): DataFrame = {
    val castedUnitDf = unitDF
      .withColumn(colName = bounds, unitDF.col(bounds).cast(LongType))
      .withColumn(colName = sic07, unitDF.col(sic07).cast(IntegerType))
      .withColumn(colName = prn, unitDF.col(prn).cast(DataTypes.createDecimalType(precision, scale)))

    if (castedUnitDf.filter(castedUnitDf(sic07).isNull || castedUnitDf(prn).isNull).count > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check common mandatory fields [$bounds, $sic07, $prn] are of expected type")
    else castedUnitDf
  }

  def checkStratifiedFrameForMandatoryFields(stratifiedDF: DataFrame): DataFrame = {
    val castedStratifiedDF = stratifiedDF
    if (castedStratifiedDF.filter(castedStratifiedDF(cellNumber).isNull ||
      castedStratifiedDF(prn).isNull).count > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check common mandatory fields [$cellNumber, $prn] are of expected type")
    else castedStratifiedDF
  }

  def checkPayeforMandatoryFields(PayeDF: DataFrame): DataFrame = {
    val castedPayeDF = PayeDF
      .withColumn(colName = payeEmployees, PayeDF.col(payeEmployees).cast(LongType))
      .withColumn(colName = paye_jobs, PayeDF.col(paye_jobs).cast(IntegerType))

    if (castedPayeDF.filter(castedPayeDF(payeEmployees).isNull || castedPayeDF(paye_jobs).isNull).count() > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check mandatory fields [$payeEmployees, $paye_jobs] are of expected type")
    else castedPayeDF
  }

  def checkVatforMandatoryFields(VatDF: DataFrame): DataFrame = {
    val castedVatDF = VatDF
      .withColumn(colName = payeEmployees, VatDF.col(payeEmployees))
      .withColumn(colName = paye_jobs, VatDF.col(paye_jobs))
      .withColumn(colName = contained, VatDF.col(contained).cast(LongType))
      .withColumn(colName = apportioned, VatDF.col(apportioned).cast(LongType))
      .withColumn(colName = standard, VatDF.col(standard).cast(LongType))
      .withColumn(colName = group_turnover, VatDF.col(group_turnover).cast(LongType))
      .withColumn(colName = ent, VatDF.col(ent).cast(LongType))

    if (castedVatDF.filter(castedVatDF(payeEmployees).isNull || castedVatDF(paye_jobs).isNull || castedVatDF(ent).isNull).count() > NullableValuesAllowed)
      throw new IllegalArgumentException(s"Check mandatory fields [$payeEmployees, $paye_jobs, $contained, $apportioned, $standard, $group_turnover, $ent] are of expected type")
    else castedVatDF
  }

  def checkGroupVatforMandatoryFields(VatDF: DataFrame): DataFrame = {
    val castedVatDF = VatDF
      .withColumn(colName = group_turnover, VatDF.col(group_turnover).cast(LongType))
    castedVatDF
  }

  def checkAppVatforMandatoryFields(VatDF: DataFrame): DataFrame = {
    val castedVatDF = VatDF
      .withColumn(colName = apportioned, VatDF.col(apportioned).cast(LongType))
    castedVatDF
  }

  def checkCntVatforMandatoryFields(VatDF: DataFrame): DataFrame = {
    val castedVatDF = VatDF
      .withColumn(colName = contained, VatDF.col(contained).cast(LongType))
    castedVatDF
  }

  def checkStdVatforMandatoryFields(VatDF: DataFrame): DataFrame = {
    val castedVatDF = VatDF
      .withColumn(colName = standard, VatDF.col(standard).cast(LongType))
    castedVatDF
  }

  def checkEmploymentforMandatoryFields(EmpDF: DataFrame): DataFrame = {
    val castedEmpDF = EmpDF
      .withColumn(colName = employment, EmpDF.col(employment).cast(LongType))
    castedEmpDF
  }

  def checkEmployeesforMandatoryFields(EmpDF: DataFrame): DataFrame = {
    val castedEmpDF = EmpDF
      .withColumn(colName = ent_empees, EmpDF.col(ent_empees).cast(LongType))
    castedEmpDF
  }
  
  def checkImputedforMandatoryFields(ImputedDF: DataFrame): DataFrame = {
    val castedImputedDF = ImputedDF
      .withColumn(colName = ern, ImputedDF.col(ern))
      .withColumn(colName = imp_turnover, ImputedDF.col(imp_turnover))
      .withColumn(colName = imp_empees, ImputedDF.col(imp_empees))
    castedImputedDF
  }

  def checkSicforMandatoryFields(SicDF: DataFrame): DataFrame = {
    val castedSicDF = SicDF
      .withColumn(colName = ern, SicDF.col(ern))
      .withColumn(colName = sic07, SicDF.col(sic07))
    castedSicDF
  }
}
