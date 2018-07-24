package uk.gov.ons.registers


sealed trait Validation[+A, +B, +E]
case class Success[A, B](optA: Option[A] = None, optB: Option[B] = None) extends Validation[A, B, Nothing]
case class Failure[E](head: E, tail: List[E] = Nil) extends Validation[Nothing, Nothing, E]

object Validation {
  def mapOption[A, B, E](va: Validation[A, B, E], vb: Validation[A, B, E], id: Int)
    (onFailure: Int => List[E] => Option[B]): Option[B] = (va, vb) match {
    case (Success(_, _), Success(_, optB)) => optB
    case (Failure(ha, ta), Failure(hb, tb)) => onFailure(id)(ha +: ta ++: hb +: tb)
    case (Failure(h, t), _) => onFailure(id)(h +: t)
    case (_, Failure(h, t)) => onFailure(id)(h +: t)
  }
}
