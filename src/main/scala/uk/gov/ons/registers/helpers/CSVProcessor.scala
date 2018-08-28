package uk.gov.ons.registers.helpers

import java.nio.file.Path

import scala.reflect.runtime.universe.TypeTag
import scala.util.Try

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

@deprecated
object CSVProcessor {
  val DefaultFileDelimiter = ","
  val CSV = "csv"
  private val Header = "header"

  @deprecated
  def export(dataFrame: DataFrame, path: Path, headerOption: Boolean): Unit =
    dataFrame
      .coalesce(numPartitions = 1)
      .write.format(CSV)
      .option(Header, headerOption)
      .mode(SaveMode.Append)
      .csv(path.toString)

  @deprecated
  def readFileAsSQLDataContainerElseException[A](readFromFileFunc: Path => A, filePathStr: Path)
    (implicit sparkSession: SparkSession): Either[Throwable, A] =
    TrySupport.toEither( Try(
      readFromFileFunc(filePathStr)
    ))

  @deprecated
  def schemaOf[A: TypeTag]: StructType =
    ScalaReflection
      .schemaFor[A]
      .dataType
      .asInstanceOf[StructType]

  @deprecated
  def readCsvFileAsDataset[A : Encoder : TypeTag](filePath: Path)(implicit sparkSession: SparkSession): Dataset[A] =
    sparkSession
      .read
      .option(Header, value = true)
      .schema(schemaOf[A])
      .csv(filePath.toString)
      .as[A]

//  def test[A : Encoder : TypeTag](df: DataFrame)(implicit sparkSession: SparkSession): Dataset[A] = {
//    df.schema(schemaOf(A)).as[A]
//  }

  @deprecated
  def readCsvFileAsDataFrame(filePath: Path)(implicit sparkSession: SparkSession): DataFrame =
    sparkSession
      .read
      .option(Header, value = true)
      .csv(filePath.toString)
}
