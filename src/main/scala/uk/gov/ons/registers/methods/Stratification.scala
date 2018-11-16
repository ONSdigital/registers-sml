package uk.gov.ons.registers.methods

import javax.inject.Singleton
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.TransformDataFrames.validateAndParseInputsStrata
import uk.gov.ons.registers.helpers.SmlLogger
import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting.checkUnitForMandatoryFields
import uk.gov.ons.registers.model.selectionstrata.StratificationPropertiesFields

@Singleton
trait Stratification extends SmlLogger{

  def stratify(inputDf: DataFrame, stratificationPropsDf: DataFrame, bounds:String)(implicit spark: SparkSession): DataFrame = {

    val defaultPartitions = inputDf.rdd.getNumPartitions
    val cellNo = StratificationPropertiesFields.cellNumber

    val dataDf = inputDf.withColumn(cellNo, lit(-2))
    dataDf.createOrReplaceTempView("datadf")
    stratificationPropsDf.createOrReplaceTempView("propsdf")
    val joinSql =
      """
        SELECT datadf.*, propsdf.cell_no AS allocated_cell_no
        FROM datadf
        LEFT JOIN propsdf ON CAST(datadf.sic07 AS INT) >= CAST(propsdf.lower_class AS INT) AND
                            CAST(datadf.sic07 AS INT) <= CAST(propsdf.upper_class AS INT) AND
                            CAST(datadf.paye_empees AS INT) >= CAST(propsdf.lower_size AS INT) AND
                            CAST(datadf.paye_empees AS INT) <= CAST(propsdf.upper_size AS INT)
      """.stripMargin

    val joinedDF = spark.sql(joinSql)

    val allocatedCellNoDF = joinedDF.withColumn(cellNo, when(col("allocated_cell_no").isNotNull, col("allocated_cell_no")).otherwise(col(cellNo)))
    val res = allocatedCellNoDF.drop(col("allocated_cell_no"))
    logPartitionInfo(res, 36, "resulted stratification DF")
    val repartitionedDF = res.coalesce(defaultPartitions)
    logPartitionInfo(repartitionedDF, 38, "repartitioned stratification DF")
    repartitionedDF
  }
}



