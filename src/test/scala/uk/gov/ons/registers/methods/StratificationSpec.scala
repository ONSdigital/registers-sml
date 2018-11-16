package uk.gov.ons.registers.methods

import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}


class StratificationSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with TestStratificationData{
  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      object MockStratification extends Stratification

      val dataDF = getDataDF
      val propsDF = getPropsDF
      val expected = expectedOutputDF.collect()
      val actual: Array[Row] = MockStratification.stratify(dataDF,propsDF,"paye_empees").orderBy("prn").collect().sortBy(_.getAs[String]("ern").toInt)
      spark.stop()
      actual shouldBe expected
    }
  }
}
