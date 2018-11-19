package uk.gov.ons.registers.methods

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import uk.gov.ons.registers.model.CommonFrameDataFields._



trait Sic extends Serializable {

  object Spark {
    val ctx = new SparkContext(new SparkConf().setAppName("test").setMaster("local[*]"))
  }

  def getClassification(df: DataFrame)(implicit activeSession: SparkSession): DataFrame = {
    val subDF = df.withColumn(division, substring(df.col(sic07), 1, 2))
                  .withColumn(employees, df.col(employees).cast(IntegerType))
    @transient
    val rddList = Spark.ctx.parallelize(df))
    def sic(input: String): String = duplicateCheck(endCalc(check(subDF.filter(col(id) isin input)))).first.getString(1)

//    println(sic("123"))
//    println(sic("345"))
//    println(sic("456"))
//    println(sic("654"))


    val idDF = df.groupBy(id).agg(count(id)).select(id)
    val coder :(String => String) = (arg: String) => sic(arg)
    val sqlFunc = udf(sic _)

    idDF.withColumn(sic07, sqlFunc(col(id))).show()
    println("scheema")
    idDF.withColumn(sic07, sqlFunc(col(id))).printSchema()




    //    println(endCalc(check(subDF)).first.getString(1))
    //
    //
    //    println(duplicateCheck(endCalc(check(subDF.filter(col(id) isin "123")))).first.getString(1))
    //    println(duplicateCheck(endCalc(check(subDF.filter(col(id) isin "345")))).first.getString(1))
    //    println(duplicateCheck(endCalc(check(subDF.filter(col(id) isin "456")))).first.getString(1))
    //    println(duplicateCheck(endCalc(check(subDF.filter(col(id) isin "654")))).first.getString(1))


    //    idDF.withColumn(sic07, duplicateCheck(endCalc(check(subDF.filter(col(id) isin idDF.col(id))))).col(sic07)).show

//    val schema = idDF.schema
//    val sqlContext = idDF.sqlContext
//    val rdd = idDF.rdd.map(
//      row =>row.toSeq.map {
//        r => duplicateCheck(subDF.filter(col(id) isin r)).first.getString(1)
//      })
//      .map(Row.fromSeq)


//    sqlContext.createDataFrame(rdd, schema).show()



//    val List1 = idDF.select(id).rdd.map(r => r(0)).collect()
//    List1.map(r => subDF.filter(col(id) isin r).first.getString(1))
//
//    val list2 = List1.map(r => duplicateCheck(endCalc(check(subDF.filter(col(id) isin r)))).first.getString(1))
//
//    val rows = list2.map{x => Row(x:_*)}
//    val rdd = sparkContext.makeRDD[RDD](rows)
//    val df2 = sqlContext.createDataFrame(rdd, schema)


    val output = duplicateCheck(endCalc(check(subDF)))

    output
  }

  private def duplicateCheck(dataFrame: DataFrame): DataFrame = {
    if(dataFrame.count() > 1) {
      val castCalc = dataFrame.withColumn(sic07, dataFrame.col(sic07).cast(IntegerType))
      val joinCalc = castCalc.agg(min(sic07) as sic07).join(dataFrame, sic07)

      joinCalc.withColumn(sic07, joinCalc.col(sic07).cast(StringType)).select(id, sic07, employees)
    }else dataFrame
  }

  private def endCalc(df: DataFrame): DataFrame = {
    val castedDF = df
    val aggDF = castedDF.groupBy(id).agg(max(employees)).withColumnRenamed(id, "id1")
    aggDF.join(castedDF, aggDF(s"max($employees)") === castedDF(employees) && aggDF("id1") === castedDF(id), "inner")
      .select(id, sic07, s"max($employees)").withColumnRenamed(s"max($employees)", employees)
  }

  private def check(dataFrame: DataFrame): DataFrame = {
    val list = dataFrame.select(division).rdd.map(r => r(0)).collect.toList
    var x = 0
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




