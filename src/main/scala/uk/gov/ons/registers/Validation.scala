package uk.gov.ons.registers

import scala.util.Try

sealed trait Validation[+A, +E]
case class Success[A](valid: A) extends Validation[Nothing, A]
case class Failure[E](head: E, tail: List[E] = Nil) extends Validation[E, Nothing]

object Validation {
  type ErrorMessage = String

  private def apply[A, B, E](vFn: Validation[E, A => B])(vA: Validation[E, A]): Validation[E, B] =
    map2(vFn, vA) {(f, a) =>
      f(a)
    }

  private def unit[E, A](a: => A): Validation[E, A] =
    Success(a)

  def toOption[A, B, C, E](va: Validation[E, A], vb: Validation[E, B])
    (onFailure: List[E] => Option[C], onSuccess: (A, B) => Option[C]): Option[C] = (va, vb) match {
    case (Success(a), Success(b)) => onSuccess(a, b)
    case (Failure(ha, ta), Failure(hb, tb)) => onFailure(ha +: ta ++: hb +: tb)
    case (Failure(h, t), _) => onFailure(h +: t)
    case (_, Failure(h, t)) => onFailure(h +: t)
  }

  def toValidation[A](aTry: Try[A]): Validation[ErrorMessage, A] =
    aTry match {
      case util.Success(v) => Success(v)
      case util.Failure(e) => Failure(e.getMessage)
    }

  def fold[A, B, E](v: Validation[E, A])(onFailure: List[E] => B, onSuccess: A => B): B =
    v match{
      case Success(s) => onSuccess(s)
      case Failure(h, t) => onFailure(h +: t)
    }

  def map2[A, B, C, E](va: Validation[E, A], vb: Validation[E, B])(f: (A, B) => C): Validation[E, C] =
    (va, vb) match {
      case (Success(a), Success(b)) => Success(f(a, b))
      case (Failure(ha, ta), Failure(hb, tb)) => Failure(ha, ta ++: hb +: tb)
      case (Failure(h, t), _) => Failure(h, t)
      case (_, Failure(h, t)) => Failure(h, t)
    }

  def map3[A, B, C, D, E](va: Validation[E, A], vb: Validation[E, B], vc: Validation[E, C])(f: (A, B, C) => D): Validation[E, D] = {
    val vbcd = apply(unit(f.curried))(va)
    val vcd = apply(vbcd)(vb)
    apply(vcd)(vc)
  }
}
