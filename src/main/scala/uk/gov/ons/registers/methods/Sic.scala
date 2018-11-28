package uk.gov.ons.registers.methods

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait Sic extends Serializable {

  def getClassification (df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val subDF = df.withColumn(division, substring(df.col(sic07), 1, 2))
                  .withColumn(employees, df.col(employees).cast(IntegerType))

    val ernDF = df.groupBy(ern).agg(count(ern)).select(ern)

    val ernList = ernDF.select(ern).rdd.map(r => r(0)).collect().map(_.asInstanceOf[String]).toList

    val lurnList = ernList.map(r => calc(subDF, r).first.getString(1))
    import spark.sqlContext.implicits._
    val listDF = lurnList.zip(ernList).toDF(lurn, ern)

    val output = listDF.join(subDF.drop(ern), lurn)
      .withColumn(classs, substring(df.col(sic07), 1, 4))
      .withColumn(group, substring(df.col(sic07), 1, 3))
      .select(ern, lurn, sic07, classs, group, division, employees)

    output
  }

  private def calc(inputDF: DataFrame, row : String)(implicit sparkSession: SparkSession): DataFrame = duplicateCheck(endCalc, check, inputDF, row)

  private def duplicateCheck(f: DataFrame => DataFrame, g: DataFrame => DataFrame, dataFrame: DataFrame, rowID: String): DataFrame = {
    val df: DataFrame = f(g(dataFrame.filter(col(ern) isin rowID)))

    if (dataFrame.count() > 1) {
      val castCalc = df.withColumn(sic07, df.col(sic07).cast(IntegerType))
      val joinCalc = castCalc.agg(min(sic07) as sic07).join(df, sic07)

      joinCalc.withColumn(sic07, joinCalc.col(sic07).cast(StringType)).select(ern, lurn, sic07, employees)
    } else df
  }

  private def endCalc(df: DataFrame): DataFrame = {
    val castedDF = df
    val aggDF = castedDF.groupBy(ern).agg(max(employees)).withColumnRenamed(ern, "ern1")
    aggDF.join(castedDF, aggDF(s"max($employees)") === castedDF(employees) && aggDF("ern1") === castedDF(ern), "inner")
      .select(ern, lurn, sic07, s"max($employees)").withColumnRenamed(s"max($employees)", employees)
  }

  private def check(dataFrame: DataFrame): DataFrame = {
    //remove duplicate sic07 codes
    val removeDuplicateSicFrame = dataFrame.groupBy(sic07).agg(sum(employees) as employees, min(lurn) as lurn).join(dataFrame.drop(employees, lurn), sic07)

    //check to see if 46 or 47 is in the DF
    val list = removeDuplicateSicFrame.select(division).rdd.map(r => r(0)).collect.toList
    if ((list contains "46") || (list contains "47")) {
      //div 46 and/or 47 is in the frame, check which is biggest
      val divSplit = subCheck(removeDuplicateSicFrame, division, 46, 47)
      divSplit match {
        case "46" => groupCheck46(removeDuplicateSicFrame, divSplit)
        case "47" => groupCheck47(removeDuplicateSicFrame, divSplit)
        case _ => removeDuplicateSicFrame.filter(not(col(division) isin (46, 47)))}
    }else removeDuplicateSicFrame
  }

   private def subCheck(dataFrame: DataFrame, sub: String, param1: Int, param2: Int): String = {
     //check whether 46, 47 or _ is the biggest division, prints 46, 47 or 1.0 respectively
     val aggSubDF = dataFrame.groupBy(sub).agg(sum(employees) as employees)
     val specDF = aggSubDF.filter(col(sub) isin (param1, param2))

     val filteredDF = aggSubDF.filter(not(col(sub) isin (param1, param2))).agg(sum(employees) as employees)
     val empFilDF = filteredDF.withColumn(sub, filteredDF.col(employees)/filteredDF.col(employees).cast(IntegerType)).select(sub, employees).union(specDF)

     val aggMergeDF = empFilDF.agg(max(employees) as employees)
     aggMergeDF.join(empFilDF, employees).select(sub).first.getString(0)
  }

   private def groupCheck46(dataFrame: DataFrame, split: String): DataFrame = {
     //checking groups inside division 46
     val groupDF = dataFrame.filter(col(division) isin split).withColumn(group, substring(dataFrame.col(sic07), 1, 3))
     val splitDFIN = groupDF.filter(col(group) isin "461")
     val splitDFNot = groupDF.filter(not(col(group) isin "461"))
     if(compareDF(splitDFIN, splitDFNot) == splitDFNot){
       val splitDFNot1 = splitDFNot.filter(not(col(group) isin "469"))
       val splitDFIn1 = splitDFNot.filter(col(group) isin "469")
       if(compareDF(splitDFNot1, splitDFIn1) == splitDFNot1) {
         val groupDF = splitDFNot1.groupBy(group).agg(sum(employees) as employees)
         val groupVal = groupDF.agg(max(employees) as employees).join(groupDF, employees).first.getString(1)
         splitDFNot1.filter(col(group) isin groupVal)
       }else splitDFIn1
     }else splitDFIN
   }

   private def groupCheck47(dataFrame: DataFrame, split: String): DataFrame = {
     //checking all the groups inside division 47
     val groupDF = dataFrame.filter(col(division).isin(split)).withColumn(group, substring(dataFrame.col(sic07), 1, 3))
     val splitDFNot = groupDF.filter(not(col(group).isin("478", "479")))
     val splitDF2IN = groupDF.filter(col(group).isin("478", "479"))

     if(compareDF(splitDFNot, splitDF2IN) == splitDFNot) {
       val sumAggDF = groupCheck471to477(splitDFNot).groupBy(group).agg(sum(employees) as employees)
       val theGroup = sumAggDF.agg(max(employees) as employees).join(sumAggDF, employees).first.getString(1)
       splitDFNot.filter(col(group).isin(theGroup))
     }else groupCheck478and479(splitDF2IN)

   }

  private def groupCheck471to477(dataFrame: DataFrame): DataFrame = {
    //a check of the groups 471-477 in division 47
    val splitDFIN = dataFrame.filter(col(group).isin("471"))
    val splitDFNot = dataFrame.filter(not(col(group).isin("471")))

    if(compareDF(splitDFIN, splitDFNot) == splitDFIN){
      val classDF = splitDFIN.withColumn(classs, substring(splitDFIN.col(sic07), 1, 4))
      val groupSplit = subCheck(classDF, classs, 4711, 4719)
      if(groupSplit == "4711" || groupSplit == "4719") {
        classDF.filter(col(classs) isin groupSplit)
      }else classDF.filter(not(col(classs) isin (4711, 4719)))
    }else{
      val sumDF = splitDFNot.groupBy(group).agg(sum(employees) as employees)
      val filter = sumDF.agg(max(employees) as employees).join(sumDF, employees).first.getString(1)
      splitDFNot.filter(col(group) isin filter)
    }
    compareDF(splitDFIN, splitDFNot)
  }

  private def groupCheck478and479(dataFrame: DataFrame): DataFrame = {
    //a check of the groups 478-479 in division 47
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




