package uk.gov.ons.registers.methods



import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class SampleSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with TestSampleData{



  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[4]").appName("enterprise assembler").getOrCreate()
      object MockSample extends Sample

      val dataDF = getDataDF
      val propsDF = getPropsDF
      val expected = expectedOutputDF.collect()
      val actual: Array[Row] = MockSample.create(dataDF,propsDF).orderBy("prn").collect()
      spark.stop()
      actual shouldBe expected
    }
  }

}
