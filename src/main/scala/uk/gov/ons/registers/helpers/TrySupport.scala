package uk.gov.ons.registers.helpers

import scala.util.{Failure, Success, Try}

/*
 * Remove this and use the standard library functions once on Scala 2.12
 */
object TrySupport {
  def toEither[A](aTry: Try[A]): Either[Throwable, A] =
    aTry match {
      case Success(s) => Right(s)
      case Failure(ex) => Left(ex)
    }

  def fold[A, B](aTry: Try[A])(onFailure: Throwable => B, onSuccess: A => B): B =
    aTry match {
      case Success(s) => onSuccess(s)
      case Failure(ex) => onFailure(ex)
    }
}
