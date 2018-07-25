package uk.gov.ons.registers.utils

import scala.util.{Failure, Success}

import org.scalatest.{FreeSpec, Matchers}

import uk.gov.ons.registers.helpers.TrySupport


class TrySupportSpec extends FreeSpec with Matchers {

  "TrySupportSpec" - {
    "when transforming a Try to an Either" - {
      "transforms a Success to a Right" - {
        val str = "Right Success"
        TrySupport.toEither(Success(str)) shouldBe Right(str)
      }

      "converts a Failure to a Left" - {
        val numberFormatException = new NumberFormatException()
        TrySupport.toEither(Failure(numberFormatException)) shouldBe Left(numberFormatException)
      }
    }
  }
}
