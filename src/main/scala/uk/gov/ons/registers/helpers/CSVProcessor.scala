package uk.gov.ons.registers.helpers

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._


object CSVProcessor {
  val CSV = "csv"
  type FilePath = String


  def export(dataFrame: DataFrame, path: FilePath, headerOption: Boolean = true): Unit =
    dataFrame
      .coalesce(1)
      .write.format(CSV)
      .option("header", headerOption)
      .mode(SaveMode.Append)
      .csv(path)

  def readFileAsSQLDataContainerElseException[A](readFromFileFunc: String => A, filePathStr: FilePath)
                                                (implicit sparkSession: SparkSession): A =
    TrySupport.toEither(
      Try(if (filePathStr.endsWith(CSV)) {
          readFromFileFunc(filePathStr)
        } else throw new Exception(s"File [$filePathStr] is not in csv format or cannot be found"))
    ).fold[A](ex => throw ex, identity)

  private def schemaOf[A: TypeTag]: StructType = {
    ScalaReflection
      .schemaFor[A]
      .dataType
      .asInstanceOf[StructType]
  }

  def readCsvFileAsDataset[A : Encoder : TypeTag](filePath: FilePath)(implicit sparkSession: SparkSession): Dataset[A] =
    sparkSession
      .read
      .option("header", "true")
      .schema(schemaOf[A])
      .csv(filePath)
      .as[A]

  def readCsvFileAsDataFrame(filePath: FilePath)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .read
      .option("header", "true")
      .csv(filePath)

}