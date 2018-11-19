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
    val dataDf = unitDF.withColumn(cellNo, lit(-2).cast(IntegerType))
    dataDf.createOrReplaceTempView("datadf")
    propsDF.createOrReplaceTempView("propsdf")
    val joinSql = """
        SELECT datadf.ern,
        datadf.entref,
        datadf.name,
        datadf.tradingstyle,
        datadf.address1,
        datadf.address2,
        datadf.address3,
        datadf.address4,
        datadf.address5,
        datadf.postcode,
        datadf.legalstatus,
        datadf.sic07,
        datadf.paye_empees,
        datadf.paye_jobs,
        datadf.ent_turnover,
        datadf.std_turnover,
        datadf.grp_turnover,
        datadf.cntd_turnover,
        datadf.app_turnover,
        datadf.prn,
        CAST(datadf.cell_no AS INT),
        CAST(propsdf.cell_no AS INT) AS allocated_cell_no
        FROM datadf
        LEFT JOIN propsdf ON datadf.sic07 >= propsdf.lower_class AND
                            sic07 <= CAST(propsdf.upper_class AS INT) AND
                            datadf.paye_empees >= propsdf.lower_size AND
                            datadf.paye_empees <= propsdf.upper_size
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



