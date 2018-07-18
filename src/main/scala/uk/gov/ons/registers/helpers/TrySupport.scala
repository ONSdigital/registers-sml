package uk.gov.ons.registers.helpers

import scala.util.{Failure, Success, Try}

object TrySupport {

  def toEither[A, B](aTry: Try[A]): Either[Throwable, A] =
    aTry match {
      case Success(s) => Right(s)
      case Failure(ex) => Left(ex)
    }
}