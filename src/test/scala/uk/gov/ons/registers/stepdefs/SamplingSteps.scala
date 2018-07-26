package uk.gov.ons.registers.stepdefs

import uk.gov.ons.registers.method.Sample
import uk.gov.ons.registers.support.AssertionHelpers._
import uk.gov.ons.registers.support.FileProcessorHelper._
import uk.gov.ons.registers.support.sample.SampleEnterpriseRow
import uk.gov.ons.registers.support.sample.SampleEnterpriseRow._
import uk.gov.ons.stepdefs.Helpers

import cucumber.api.scala.{EN, ScalaDsl}
//import org.junit.Assert._

class SamplingSteps extends ScalaDsl with EN{

  private def expectedFirstRow(cellNumber: String) =
    SampleEnterpriseRow(ern = "1100000241", entref = "9999999999", name = "9337DT",
      address1 = "1 BORLASES COTTAGES", address2 = "MILLEY ROAD", address3 = "WALTHAM ST LAWRENCE", address4 = "READING",
      postcode = "RG10 0JY", legalstatus= "1", sic07 = "45400", paye_empees= "38", paye_jobs= "34",ent_turnover= "2184",
      std_turnover= "2184", grp_turnover= "0", cntd_turnover= "0", app_turnover= "0", prn = "0.129854883", cell_no = cellNumber)

  private val frameSize = 1000L

  //TODO - pass sparkSession implicit
  When("""a Scala Sample is created from the pre-filtered frame"""){ () =>
    outputDataDF = Sample.sample(inputPath = inputPath)(sparkSession = Helpers.sparkSession)
      .create(stratificationPropertiesPath, outputPath)
  }

  Then("""a Sample DataFrame containing the Census strata is returned and exported to CSV"""){ () =>
    val censusSampleCsvFile = assertAndReturnCsvOfSampleCollection
    assertSampleCollectionSize(sampleCollectionCsv = censusSampleCsvFile, expectedNumberOfRecords = frameSize)
    assertNewCellNumberFieldHasBeenAdded(censusSampleCsvFile)

    val expectedCensusFirstRow = SampleEnterpriseRow(ern = "1100000001", entref = "9906000015", name = "&EAGBBROWN",
      address1 = "1 HAWRIDGE HILL COTTAGES", address2 = "THE VALE", address3 = "HAWRIDGE", address4 = "CHESHAM BUCKINGHAMSHIRE",
      postcode = "HP5 3NU", legalstatus= "1", sic07 = "45112", paye_empees= "1", paye_jobs= "1",ent_turnover= "73",
      std_turnover= "73", grp_turnover= "0", cntd_turnover= "0", app_turnover= "0", prn = "0.109636832", cell_no = "5814"
    )
    assertRowEquality(csvRow = enterpriseRowFromMap(csvRowAsColumnsWithMap = lineWithHeaderAsMap(censusSampleCsvFile)), expectedRow = expectedCensusFirstRow)

    displayData(expectedRow = expectedCensusFirstRow)
  }

  Then("""a Sample DataFrame containing the Prn-Sampling strata is returned and exported to CSV"""){ () =>
    val prnSampleCsvFile = assertAndReturnCsvOfSampleCollection
    assertSampleCollectionSize(sampleCollectionCsv = prnSampleCsvFile, expectedNumberOfRecords = 4)
    assertNewCellNumberFieldHasBeenAdded(prnSampleCsvFile)

    val expectedPrnSampleFirstRow = expectedFirstRow(cellNumber="5813")
    assertRowEquality(csvRow = enterpriseRowFromMap(csvRowAsColumnsWithMap = lineWithHeaderAsMap(prnSampleCsvFile)), expectedRow = expectedPrnSampleFirstRow)

    displayData(expectedRow = expectedPrnSampleFirstRow)
  }

  Then("""a Sample DataFrame is returned and exported to CSV with the inclusion of strata with outbound Sample Size parameter"""){ () =>
    val prnSampleWithOutOfBoundsSampleSizeCsvFile = assertAndReturnCsvOfSampleCollection
    assertSampleCollectionSize(sampleCollectionCsv = prnSampleWithOutOfBoundsSampleSizeCsvFile, expectedNumberOfRecords = 2)
    assertNewCellNumberFieldHasBeenAdded(prnSampleWithOutOfBoundsSampleSizeCsvFile)

    val firstRowPrn = outputDataDF.first.getAs[BigDecimal](prn)
    val secondRowPrn = outputDataDF.take(2).last.getAs[BigDecimal](prn)
    assert(firstRowPrn > secondRowPrn, message = s"expected prn of first row [$firstRowPrn] in DataFrame to be higher than prn of second row [$secondRowPrn]")
  }

  Then("""a Sample DataFrame is returned and exported to CSV, with the invalid Sample Size strata logged and entire frame returned for that strata"""){ () =>
    val sampleSizeTooGreatCsvFile = assertAndReturnCsvOfSampleCollection
    assertSampleCollectionSize(sampleCollectionCsv = sampleSizeTooGreatCsvFile, expectedNumberOfRecords = frameSize)
    assertNewCellNumberFieldHasBeenAdded(sampleSizeTooGreatCsvFile)

    val expectedSampleWithSampleSizeTooGreatFirstRow = expectedFirstRow(cellNumber="5899")
    assertRowEquality(csvRow = enterpriseRowFromMap(csvRowAsColumnsWithMap = lineWithHeaderAsMap(sampleSizeTooGreatCsvFile)),
      expectedRow = expectedSampleWithSampleSizeTooGreatFirstRow)

    displayData(expectedRow = expectedSampleWithSampleSizeTooGreatFirstRow)
  }
}
