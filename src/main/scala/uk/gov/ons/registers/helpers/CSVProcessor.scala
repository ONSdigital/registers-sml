package uk.gov.ons.registers.helpers

import java.nio.file.Path

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType


object CSVProcessor {
  val CSV = "csv"
  type FilePath = Path

  def export(dataFrame: DataFrame, path: Path, headerOption: Boolean = true): Unit =
    dataFrame
      .coalesce(numPartitions = 1)
      .write.format(CSV)
      .option("header", headerOption)
      .mode(SaveMode.Append)
      .csv(path.toString)

  @deprecated("Migrate to readFileAsSQLDataContainerElseException")
  def readFileAsSQLDataContainerElseExceptionOLD[A](readFromFileFunc: Path => A, filePathStr: Path)(implicit sparkSession: SparkSession): A =
    TrySupport.toEither(
      Try(if (filePathStr.endsWith(CSV)) readFromFileFunc(filePathStr)
        else throw new Exception(s"File [$filePathStr] is not in csv format or cannot be found")))
      .fold[A](ex => throw ex, identity)


  def readFileAsSQLDataContainerElseException[A](readFromFileFunc: Path => A, filePathStr: Path)
  (implicit sparkSession: SparkSession): Either[Throwable, A] =
    TrySupport.toEither( Try(
      readFromFileFunc(filePathStr)
    ))

  private def schemaOf[A: TypeTag]: StructType =
    ScalaReflection
      .schemaFor[A]
      .dataType
      .asInstanceOf[StructType]

  def readCsvFileAsDataset[A : Encoder : TypeTag](filePath: Path)(implicit sparkSession: SparkSession): Dataset[A] =
    sparkSession
      .read
      .option("header", value = true)
      .schema(schemaOf[A])
      .csv(filePath.toString)
      .as[A]

  def readCsvFileAsDataFrame(filePath: Path)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .read
      .option("header", value = true)
      .csv(filePath.toString)

}
