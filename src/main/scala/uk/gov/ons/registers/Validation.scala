package uk.gov.ons.registers


sealed trait Validation[+A, +E]
case class Success[A](valid: A) extends Validation[A, Nothing]
case class Failure[E](head: E, tail: List[E] = Nil) extends Validation[Nothing, E]

object Validation {
  def toOption[A, B, C, E](va: Validation[A, E], vb: Validation[B, E])
    (onFailure: List[E] => Option[C], onSuccess: (A, B) => Option[C]): Option[C] = (va, vb) match {
    case (Success(validA), Success(validB)) => onSuccess(validA, validB)
    case (Failure(ha, ta), Failure(hb, tb)) => onFailure(ha +: ta ++: hb +: tb)
    case (Failure(h, t), _) => onFailure(h +: t)
    case (_, Failure(h, t)) => onFailure(h +: t)
  }
}
