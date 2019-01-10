package uk.gov.ons.registers.methods

import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ons.registers.model.CommonFrameDataFields._

trait Sic extends Serializable {

  def getClassification(dataFrame: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val df = dataFrame.withColumn(division, substring(dataFrame.col(sic07), 1, 2))
      .withColumn(employees, dataFrame.col(employees).cast(IntegerType))

    val ernDF = dataFrame.groupBy(ern).agg(count(ern)).select(ern)
    val ernList = ernDF.select(ern).rdd.map(r => r(0)).collect().map(_.asInstanceOf[String]).toList

    val lurnList = ernList.map(r => calc(df, r).first.getString(0))
    import spark.sqlContext.implicits._
    val listDF = lurnList.zip(ernList).toDF(sic07, ern).select(ern, sic07)

    listDF
  }

  private def calc(dataFrame: DataFrame, row: String)(implicit sparkSession: SparkSession): DataFrame = {
    val checkedDF = check(dataFrame, row)
    duplicateCheck(checkedDF.agg(max(employees) as employees).join(checkedDF, employees), sic07).select(sic07)
  }

  private def check(inputDF: DataFrame, row: String): DataFrame = {
    //filter for each ern
    val filterDF = inputDF.filter(col(ern) isin row)
    val removeDuplicateSicFrame = filterDF.groupBy(sic07).agg(sum(employees) as employees, min(lurn) as lurn).join(filterDF.drop(employees, sic07), lurn)

    //only apply calculation if there is more than one local unit
    if (removeDuplicateSicFrame.count() > 1) {
      val subdivisionDF = subdivisionCheck(removeDuplicateSicFrame)

      if (subdivisionDF.count() > 1) {
        val divisionDF = divisionCheck(subdivisionDF)

        if (divisionDF.count > 1) {
          //if division is 46/47 it is a special case
          divisionDF.first.getString(0) match {
            case "46" => {
              val groupdf = groupCheck46(divisionDF)

              if (groupdf.count > 1) {
                classCheck(groupdf)
              } else groupdf
            }
            case "47" => {
              val groupdf = groupCheck47(divisionDF)

              if (groupdf.count > 1) {
                classCheck(groupdf)
              } else groupdf
            }
            case _ => {
              val groupdf = groupCheck(divisionDF)

              if (groupdf.count > 1) {
                classCheck(groupdf)
              } else groupdf
            }
          }
        } else divisionDF
      } else subdivisionDF
    } else filterDF
  }

  private def subdivisionCheck(dataFrame: DataFrame): DataFrame = {
    //changed the subdivision classing to numbers for the duplicate check
    val myUDF = udf((code: Int) => {
      //https://collaborate2.ons.gov.uk/confluence/pages/viewpage.action?pageId=5386186
      //link to the subdivision letter groupings on confluence
      code match {
        case _ if 1 to 3 contains code => 'A'
        case _ if 5 to 9 contains code => 'B'
        case _ if 10 to 33 contains code => 'C'
        case 35 => 'D'
        case _ if 36 to 39 contains code => 'E'
        case _ if 41 to 43 contains code => 'F'
        case _ if 45 to 47 contains code => 'G'
        case _ if 49 to 53 contains code => 'H'
        case _ if 55 to 56 contains code => 'I'
        case _ if 58 to 63 contains code => 'J'
        case _ if 64 to 66 contains code => 'K'
        case 68 => 'L'
        case _ if 69 to 75 contains code => 'M'
        case _ if 77 to 84 contains code => 'N'
        case 84 => 'O'
        case 85 => 'P'
        case _ if 86 to 88 contains code => 'Q'
        case _ if 90 to 93 contains code => 'R'
        case _ if 94 to 96 contains code => 'S'
        case _ if 97 to 98 contains code => 'T'
        case 99 => 'U'
      }
    }.toInt)

    val dfWithSubdivision = dataFrame.withColumn(subdivision, (myUDF(dataFrame(division))))
    val dfWithAggSubdivision = dfWithSubdivision.groupBy(subdivision).agg(sum(employees) as employees)
    val dupCheckedDF = duplicateCheck(dfWithAggSubdivision.agg(max(employees) as employees).join(dfWithAggSubdivision, employees), subdivision).drop(employees)

    dfWithSubdivision.filter(col(subdivision) isin dupCheckedDF.first.getString(0)).drop(subdivision)
  }

  private def duplicateCheck(dataFrame: DataFrame, column: String): DataFrame = {
    if (dataFrame.count() > 1) {
      val castCalc = dataFrame.withColumn(column, dataFrame.col(column).cast(IntegerType))
      val joinCalc = castCalc.agg(min(column) as column).join(dataFrame, column)

      joinCalc.withColumn(column, joinCalc.col(column).cast(StringType))
    } else dataFrame.withColumn(column, dataFrame.col(column).cast(StringType))

  }

  private def divisionCheck(dataFrame: DataFrame): DataFrame = {
    val dfWithAggDivision = dataFrame.groupBy(division).agg(sum(employees) as employees)
    val dupCheckedDF = duplicateCheck(dfWithAggDivision.agg(max(employees) as employees).join(dfWithAggDivision, employees), division)
    dupCheckedDF.drop(employees).join(dataFrame, division)
  }

  private def groupCheck46(dataFrame: DataFrame): DataFrame = {
    //checking groups inside division 46
    val groupDF = dataFrame.withColumn(group, substring(dataFrame.col(sic07), 1, 3))
    val splitDFIN = groupDF.filter(col(group) isin "461")
    val splitDFNot = groupDF.filter(not(col(group) isin "461"))
    if (compareDF(splitDFIN, splitDFNot) == splitDFNot) {
      val splitDFNot1 = splitDFNot.filter(not(col(group) isin "469"))
      val splitDFIn1 = splitDFNot.filter(col(group) isin "469")
      if (compareDF(splitDFNot1, splitDFIn1) == splitDFNot1) {
        val groupDF = splitDFNot1.groupBy(group).agg(sum(employees) as employees)
        val groupVal = groupDF.agg(max(employees) as employees).join(groupDF, employees).first.getString(1)
        splitDFNot1.filter(col(group) isin groupVal)
      } else splitDFIn1
    } else splitDFIN
  }

  private def groupCheck47(dataFrame: DataFrame): DataFrame = {
    //checking all the groups inside division 47
    val groupDF = dataFrame.withColumn(group, substring(dataFrame.col(sic07), 1, 3))
    val splitDFNot = groupDF.filter(not(col(group).isin("478", "479")))
    val splitDF2IN = groupDF.filter(col(group).isin("478", "479"))

    if (compareDF(splitDFNot, splitDF2IN) == splitDFNot) {
      val sumAggDF = groupCheck471to477(splitDFNot).groupBy(group).agg(sum(employees) as employees)
      val theGroup = sumAggDF.agg(max(employees) as employees).join(sumAggDF, employees).first.getString(1)
      splitDFNot.filter(col(group).isin(theGroup))
    } else groupCheck478and479(splitDF2IN)
  }

  private def groupCheck471to477(dataFrame: DataFrame): DataFrame = {
    //a check of the groups 471-477 in division 47
    val splitDFIN = dataFrame.filter(col(group).isin("471"))
    val splitDFNot = dataFrame.filter(not(col(group).isin("471")))

    if (compareDF(splitDFIN, splitDFNot) == splitDFNot) {
      val sumDF = splitDFNot.groupBy(group).agg(sum(employees) as employees)
      val filter = duplicateCheck(sumDF.agg(max(employees) as employees).join(sumDF, employees), group).first.getString(1)
      splitDFNot.filter(col(group) isin filter)
    } else {
      splitDFIN.withColumn(classs, substring(splitDFIN.col(sic07), 1, 4))
    }
  }

  private def groupCheck478and479(dataFrame: DataFrame): DataFrame = {
    //a check of the groups 478-479 in division 47
    val splitDF1 = dataFrame.filter(col(group).isin("478"))
    val splitDF2 = dataFrame.filter(col(group).isin("479"))
    compareDF(splitDF1, splitDF2)
  }

  private def groupCheck(dataFrame: DataFrame): DataFrame = {
    val dfWithGroups = dataFrame.withColumn(group, col(sic07).substr(0, 3))
    val dfWithAggGroup = dfWithGroups.groupBy(group).agg(sum(employees) as employees)
    val dupCheckedDF = duplicateCheck(dfWithAggGroup.agg(max(employees) as employees).join(dfWithAggGroup, employees), group)
    dupCheckedDF.drop(employees).join(dfWithGroups, group).drop(group)
  }

  private def classCheck(dataFrame: DataFrame): DataFrame = {
    val dfWithClass = dataFrame.withColumn(classs, col(sic07).substr(0, 4))
    val dfWithAggClass = dfWithClass.groupBy(classs).agg(sum(employees) as employees)
    val dupCheckedDF = duplicateCheck(dfWithAggClass.agg(max(employees) as employees).join(dfWithAggClass, employees), classs)
    dupCheckedDF.drop(employees).join(dfWithClass, classs).drop(classs)
  }

  private def compareDF(dataFrame1: DataFrame, dataFrame2: DataFrame): DataFrame = {
    if (dataFrame1.head(1).isEmpty) dataFrame2
    else if (dataFrame2.head(1).isEmpty) dataFrame1
    else {
      if (dataFrame1.agg((sum(employees))).first.getLong(0) >= dataFrame2.agg((sum(employees))).first.getLong(0)) dataFrame1
      else dataFrame2
    }
  }
}