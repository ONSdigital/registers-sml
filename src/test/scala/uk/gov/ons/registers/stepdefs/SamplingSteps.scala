package uk.gov.ons.registers.stepdefs

import org.apache.spark.sql.{Dataset, Row}

import uk.gov.ons.registers.helpers.CSVProcessor.CSV
import uk.gov.ons.registers.method.Sample
import uk.gov.ons.registers.model.CommonUnitFrameDataFields.prn
import uk.gov.ons.registers.model.stratification.StratificationPropertiesFields
import uk.gov.ons.registers.support.FileProcessorHelper._
import uk.gov.ons.registers.support.sample.SampleEnterpriseRow
import uk.gov.ons.registers.support.sample.SampleEnterpriseRow._

import cucumber.api.scala.{EN, ScalaDsl}


class SamplingSteps extends ScalaDsl with EN {

  private def getAllRowsForCellNumber(cellNumber: String): Dataset[Row] =
    outputDataDF.filter(outputDataDF(StratificationPropertiesFields.cellNumber) === cellNumber)

  private def valueOrNull(str: String): String =
    Option(str.isEmpty).fold[String](empty)(_ => str)

  private def getField[A](map: Map[String, A])(fieldStr: String): A =
    map.getOrElse(key = valueOrNull(fieldStr), default = throw new NoSuchElementException(s"Could not find field " +
      s"[$fieldStr] in map [$map]"))

  //TODO - pass sparkSession implicit
  When("""the Scala sampling method is ran on the pre-filtered input"""){ () =>
    outputDataDF = Sample.sample(inputPath)
      .create(stratificationPropertiesPath, outputPath)
  }

  Then("""a DataFrame of given sample size is returned"""){ () =>
    // Assert if CSV file is saved - distributed, and thus cannot use fixed naming match
    val sampleOutputDir = new java.io.File(outputPath)
    assert(sampleOutputDir.exists && sampleOutputDir.isDirectory)
    val listOfCsvOutputFiles = sampleOutputDir.listFiles.filter(_.getName.endsWith(s".$CSV"))
    assert(listOfCsvOutputFiles.nonEmpty)
    val outputCsv = listOfCsvOutputFiles.head

    // Assert if result DataFrame and csv output produces the right number of records in total
    val expectedNumberOfRecords = 6046L
    assert(outputDataDF.count == expectedNumberOfRecords)
    val numberOfRowsInFile = getFileLength(outputCsv)
    assert(numberOfRowsInFile == outputDataDF.count)

    // Assert if new df schema and CSV output has new column - request identifier (cell_no)
    val dfSchema = outputDataDF.schema.fieldNames
    assert(dfSchema contains StratificationPropertiesFields.cellNumber)
    val csvHeaders = getFileHeader(outputCsv)
    assert(csvHeaders contains StratificationPropertiesFields.cellNumber)

    // Assert a row is captured correctly
    val expectedData: Row = SampleEnterpriseRow(ern = "1100000241", entref = "9999999999", name = "9337DT",
      address1 = "1 BORLASES COTTAGES", address2 = "MILLEY ROAD", address3 = "WALTHAM ST LAWRENCE", address4 = "READING",
      postcode = "RG10 0JY", legalstatus= "1", sic07 = "45400", paye_empees= "38", paye_jobs= "34",ent_turnover= "2184",
      std_turnover= "2184", grp_turnover= "0", cntd_turnover= "0", app_turnover= "0", prn = "0.129854883", cell_no = "5813"
    )
    assert(outputDataDF.first.toSeq equals expectedData.toSeq, message = s"actual output [${outputDataDF.first}] did not equal expected row [$expectedData]")
    val csvLineAsColumns = getFirstLineAsColumnOfFile(outputCsv)
    val csvColumnsAsMap = (dfSchema zip csvLineAsColumns).toMap
    val parseStrFieldAsRowField = getField(csvColumnsAsMap) _
    val csvRowAsDataFrameRow: Row = SampleEnterpriseRow(
      ern =parseStrFieldAsRowField(ern), entref=parseStrFieldAsRowField(entref), name=parseStrFieldAsRowField(name),
      tradingstyle=parseStrFieldAsRowField(tradingstyle), address1=parseStrFieldAsRowField(address1),
      address2=parseStrFieldAsRowField(address2), address3=parseStrFieldAsRowField(address3),
      address4=parseStrFieldAsRowField(address4), address5=parseStrFieldAsRowField(address5),
      postcode=parseStrFieldAsRowField(postcode), legalstatus=parseStrFieldAsRowField(legalstatus),
      sic07=parseStrFieldAsRowField(sic07), paye_empees=parseStrFieldAsRowField(paye_empees),
      paye_jobs=parseStrFieldAsRowField(paye_jobs), ent_turnover=parseStrFieldAsRowField(ent_turnover),
      std_turnover= parseStrFieldAsRowField(std_turnover), grp_turnover=parseStrFieldAsRowField(grp_turnover),
      cntd_turnover=parseStrFieldAsRowField(cntd_turnover), app_turnover=parseStrFieldAsRowField(app_turnover),
      prn=parseStrFieldAsRowField(prn), cell_no=parseStrFieldAsRowField(cell_no)
    )
    assert(csvRowAsDataFrameRow.toSeq equals expectedData.toSeq, message = s"actual csv row stored [$csvRowAsDataFrameRow] did not equal expected row [$expectedData]")

    // Assert if expected sample size for given request is returned and labelled
    val prnSamplingCellNumber = "5813"
    val expectedPrnSamplingSampleSize = 4
    val censusSamplingNumber = "5814"
    val expectedCensusSampleSize = 1000 // minus header
    val sampleReturnedForPrnsample = getAllRowsForCellNumber(prnSamplingCellNumber)
    assert(sampleReturnedForPrnsample.count == expectedPrnSamplingSampleSize, message = s"Actual sample size " +
      s"[${sampleReturnedForPrnsample.count}] for census [$prnSamplingCellNumber] was not equal to expected $expectedPrnSamplingSampleSize")
    val sampleReturnedForCensus = getAllRowsForCellNumber(censusSamplingNumber)
    assert(sampleReturnedForCensus.count == expectedCensusSampleSize, message = s"Actual sample size " +
      s"[${sampleReturnedForCensus.count}] for census [$censusSamplingNumber] was not equal to expected $expectedCensusSampleSize")
    val sampleSizeInFileFor = sampleSizeOfCellNumberInFile(outputCsv) _
    val actualPrnSamplingSampleSize = sampleSizeInFileFor(prnSamplingCellNumber)
    assert(actualPrnSamplingSampleSize == expectedPrnSamplingSampleSize, message = s"sample size of census cell number " +
      s"[$prnSamplingCellNumber] actual size [$actualPrnSamplingSampleSize] did not equal expected size [$expectedPrnSamplingSampleSize]")
    val actualCensusSampleSize = sampleSizeInFileFor(censusSamplingNumber)
    assert(actualCensusSampleSize == expectedCensusSampleSize, message = s"sample size of census cell number " +
      s"[$prnSamplingCellNumber] actual size [$actualCensusSampleSize] did not equal expected size [$expectedPrnSamplingSampleSize]")

    println("Compare Rows")
    println("Expected First Row Output")
    println(expectedData)
    println("Actual First Row [DF] Output")
    println(outputDataDF.first)

    println("Scala Sampling output")
    outputDataDF.show()
  }

}
