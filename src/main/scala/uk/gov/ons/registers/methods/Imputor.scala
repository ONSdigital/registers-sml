package uk.gov.ons.registers.methods

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import uk.gov.ons.spark.sql._

trait Imputor {
/**
  * returns tuple representing imp_empees, imp_turnover
  * */
  def imputeEmployees(turnover:Option[String], payeEmployees:Option[String], tph:Option[String]):(String,String) = {
    val inputs = (turnover,payeEmployees,tph)

      try{
        inputs match{
        case (Some(trn),None,Some(tph)) => ((trn.toInt / tph.toInt).toString,null)
        case (None,Some(emps),Some(tph)) => (null,(emps.toInt / tph.toInt).toString)
        case (Some(trn),Some(emps),Some(tph)) => ((trn.toInt / tph.toInt).toString,(emps.toInt / tph.toInt).toString)
        case (None,None,_) => (null,null)
        case (_,_,None) => throw new IllegalArgumentException("tph is null, retruning turple (imputed turnover = null, imputed employees = null)")
    }}catch{
        case iae: IllegalArgumentException => (null,null)
      }

  }

  val imputedSchema = new StructType()
    .add(StructField("ern", StringType,false))
    .add(StructField("imp_turnover", StringType,true))
    .add(StructField("imp_empees", StringType,true))

  def imputeTurnoverAndEmpees(df:DataFrame, tphDF:DataFrame)(implicit spark: SparkSession):DataFrame = {

    val withTphDF: DataFrame = df.join(tphDF,Seq("sic07"), "left_outer")

    val imputedDS:RDD[Row] = withTphDF.rdd.map(row => {

      val (trn,emps) = imputeEmployees(row.getOption[String]("turnover"),row.getOption[String]("paye_empees"), row.getOption[String]("tph"))

      new GenericRowWithSchema(Array(

         row.getAs[String]("ern"),
         trn,
         emps

    ),imputedSchema)})
    spark.createDataFrame(imputedDS,imputedSchema)
  }

}
