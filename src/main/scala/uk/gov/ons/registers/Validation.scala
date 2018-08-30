package uk.gov.ons.registers

import scala.util.Try

sealed trait Validation[+A, +E]
case class Success[A](valid: A) extends Validation[Nothing, A]
case class Failure[E](head: E, tail: List[E] = Nil) extends Validation[E, Nothing]

object Validation {
  type ErrorMessage = String

  def toOption[A, B, C, E](va: Validation[E, A], vb: Validation[E, B])
    (onFailure: List[E] => Option[C], onSuccess: (A, B) => Option[C]): Option[C] = (va, vb) match {
    case (Success(validA), Success(validB)) => onSuccess(validA, validB)
    case (Failure(ha, ta), Failure(hb, tb)) => onFailure(ha +: ta ++: hb +: tb)
    case (Failure(h, t), _) => onFailure(h +: t)
    case (_, Failure(h, t)) => onFailure(h +: t)
  }

  def toValidation[A](aTry: Try[A]): Validation[ErrorMessage, A] =
    aTry match {
      case util.Success(v) => Success(v)
      case util.Failure(ex) => Failure(ex.getMessage)
    }
}
