package uk.gov.ons.registers.methods

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait Sic extends Serializable {


  def getClassification (df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val subDF = df.withColumn(division, substring(df.col(sic07), 1, 2))
                  .withColumn(employees, df.col(employees).cast(IntegerType))

    val ernDF = df.groupBy(ern).agg(count(ern)).select(ern)

    def calc(inputDF: DataFrame, row : String): DataFrame = duplicateCheck(endCalc(check(getGroupedByElement(inputDF, row))))


    val ernList = ernDF.select(ern).rdd.map(r => r(0)).collect().map(_.asInstanceOf[String]).toList
    val sicList = ernList.map(r => calc(subDF, r).first.getString(1))
    import spark.sqlContext.implicits._

    val dfList = sicList.zip(ernList).toDF(lurn, ern)

    val output = dfList.join(subDF.drop(ern), lurn)
      .withColumn(classs, substring(df.col(sic07), 1, 4))
      .withColumn(group, substring(df.col(sic07), 1, 3))
      .select(ern, lurn, sic07, classs, group, division, employees)


    output
  }

  private def getGroupedByElement(dataFrame: DataFrame, rowID: String, tableName: String = "Sic")(implicit activeSession: SparkSession): DataFrame = {
    val filDF = dataFrame.select(ern, lurn, sic07, employees, division)

    filDF.filter(col(ern) isin rowID)
  }

  private def duplicateCheck(dataFrame: DataFrame): DataFrame = {
    if(dataFrame.count() > 1) {
      val castCalc = dataFrame.withColumn(sic07, dataFrame.col(sic07).cast(IntegerType))
      val joinCalc = castCalc.agg(min(sic07) as sic07).join(dataFrame, sic07)

      joinCalc.withColumn(sic07, joinCalc.col(sic07).cast(StringType)).select(ern, lurn, sic07, employees)
    }else dataFrame
  }

  private def endCalc(df: DataFrame): DataFrame = {
    val castedDF = df
    val aggDF = castedDF.groupBy(ern).agg(max(employees)).withColumnRenamed(ern, "ern1")
    aggDF.join(castedDF, aggDF(s"max($employees)") === castedDF(employees) && aggDF("ern1") === castedDF(ern), "inner")
      .select(ern, lurn, sic07, s"max($employees)").withColumnRenamed(s"max($employees)", employees)
  }

  private def check(dataFrame1: DataFrame): DataFrame = {
//    dataFrame1.show
//    dataFrame1.groupBy(sic07).agg(sum(employees) as employees, min(lurn) as lurn).join(dataFrame1.drop(employees, lurn), sic07).show()
    val dataFrame = dataFrame1.groupBy(sic07).agg(sum(employees) as employees, min(lurn) as lurn).join(dataFrame1.drop(employees, lurn), sic07)

    val list = dataFrame.select(division).rdd.map(r => r(0)).collect.toList
    var x = 0
    var subDivision = "C"
    //check to see if 46 or 47 is in the DF
    if (list contains ("46")) x = 1
    else if (list contains ("47")) x = 1

    if (x > 0) {
      //46 or 47 is in the frame
      val x = (46, 47)
      val divSplit = subCheck(dataFrame, division, 46, 47)
      if (divSplit == "46") {
        val groupDF = dataFrame.filter(col(division) isin divSplit).withColumn(group, substring(dataFrame.col(sic07), 1, 3))
        val splitDF1 = groupDF.filter(col(group) isin "461")
        val splitDF2 =groupDF.filter(not(col(group) isin "461"))
        if(compareDF(splitDF1, splitDF2) == splitDF2){
          val splitDF3 = splitDF2.filter(not(col(group) isin "469"))
          val splitDF4 = splitDF2.filter(col(group) isin "469")
          if(compareDF(splitDF3, splitDF4) == splitDF3) {
            val groupDF = splitDF3.groupBy(group).agg(sum(employees) as employees)
            val groupVal = groupDF.agg(max(employees) as employees).join(groupDF, employees).first.getString(1)
            splitDF3.filter(col(group) isin groupVal)

          }else splitDF4
        }else splitDF1

      }else if (divSplit == "47"){
        //case for 47
        val groupSplit = (groupCheck47(dataFrame, divSplit))
        groupSplit
      }else{
        //for not in 46 or 47
        dataFrame.filter(not(col(division) isin (46, 47)))}
    }else{dataFrame}
  }

   private def subCheck(dataFrame: DataFrame, sub: String, param1: Int, param2: Int): String = {
    val aggSubDF = dataFrame.groupBy(sub).agg(sum(employees) as employees)

    val specDF = aggSubDF.filter(col(sub).isin(param1, param2))

    val filteredDF = aggSubDF.filter(not(col(sub).isin(param1, param2))).agg(sum(employees) as employees)
    val empFilDF = filteredDF.withColumn(sub, filteredDF.col(employees)/filteredDF.col(employees).cast(IntegerType)).select(sub, employees)

    val mergeDF = empFilDF.union(specDF)
    val aggMergeDF = mergeDF.agg(max(employees) as employees)
    val res = aggMergeDF.join(mergeDF, employees).select(sub).first.getString(0)
    res
  }

   private def groupCheck47(dataFrame: DataFrame, split: String): DataFrame = {
     val groupDF = dataFrame.filter(col(division).isin(split)).withColumn(group, substring(dataFrame.col(sic07), 1, 3))
     val splitDF1 = groupDF.filter(not(col(group).isin("478", "479")))
     val splitDF2 = groupDF.filter(col(group).isin("478", "479"))

     if(compareDF(splitDF1, splitDF2) == splitDF1) {
       val sumAggDF = groupCheck47_17(splitDF1).groupBy(group).agg(sum(employees) as employees)
       val theGroup = sumAggDF.agg(max(employees) as employees).join(sumAggDF, employees).first.getString(1)
       splitDF1.filter(col(group).isin(theGroup))
     }else groupCheck47_89(splitDF2)

   }

  private def groupCheck47_17(dataFrame: DataFrame): DataFrame = {
    //47.1-47.7
    val splitDF1 = dataFrame.filter(col(group).isin("471"))
    val splitDF2 = dataFrame.filter(not(col(group).isin("471")))

    if(compareDF(splitDF1, splitDF2) == splitDF1){
      val classDF = splitDF1.withColumn(classs, substring(splitDF1.col(sic07), 1, 4))
      val groupSplit = subCheck(classDF, classs, 4711, 4719)

      if(groupSplit == "4711" || groupSplit == "4719") {
        classDF.filter(col(classs) isin groupSplit)
      }else classDF.filter(not(col(classs) isin (4711, 4719)))
    }
    else{
      val sumDF = splitDF2.groupBy(group).agg(sum(employees) as employees)
      val filter = sumDF.agg(max(employees) as employees).join(sumDF, employees).first.getString(1)
      splitDF2.filter(col(group) isin filter)
    }
    compareDF(splitDF1, splitDF2)
  }

  private def groupCheck47_89(dataFrame: DataFrame): DataFrame = {
    //47.8-47.9
    val splitDF1 = dataFrame.filter(col(group).isin("478"))
    val splitDF2 = dataFrame.filter(col(group).isin("479"))

    compareDF(splitDF1, splitDF2)
  }

  private def compareDF(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    if(dataFrame1.head(1).isEmpty) dataFrame2
    else if(dataFrame2.head(1).isEmpty) dataFrame1
    else {
      if (dataFrame1.agg((sum(employees))).first.getLong(0) > dataFrame2.agg((sum(employees))).first.getLong(0)) dataFrame1
      else dataFrame2
    }
  }



}




