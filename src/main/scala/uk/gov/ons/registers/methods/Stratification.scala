package uk.gov.ons.registers.methods

import javax.inject.Singleton
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.helpers.SmlLogger
import uk.gov.ons.registers.model.selectionstrata.{StratificationPropertiesFields, StratificatonPropertyFieldsCasting}
import uk.gov.ons.registers.model.CommonFrameAndPropertiesFieldsCasting._


@Singleton
trait Stratification extends SmlLogger{

  def stratify(inputDf: DataFrame, stratificationPropsDf: DataFrame, bounds:String)(implicit spark: SparkSession): DataFrame = {

    val defaultPartitions = inputDf.rdd.getNumPartitions
    val cellNo = StratificationPropertiesFields.cellNumber
    val unitDF = checkUnitForMandatoryFields(inputDf,bounds)
    val propsDF = StratificatonPropertyFieldsCasting.mapStartificationProperties(stratificationPropsDf)
    val dataDf = unitDF.withColumn(cellNo, when(col(bounds).isNull,-2).otherwise(-1).cast(IntegerType))
    dataDf.createOrReplaceTempView("datadf")
    propsDF.createOrReplaceTempView("propsdf")
    val joinSql = s"""
        SELECT datadf.*,
        CAST(propsdf.cell_no AS INT) AS allocated_cell_no
        FROM datadf
        LEFT JOIN propsdf ON datadf.sic07 >= propsdf.lower_class AND
                            sic07 <= CAST(propsdf.upper_class AS INT) AND
                            datadf.$bounds >= propsdf.lower_size AND
                            datadf.$bounds <= propsdf.upper_size
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



