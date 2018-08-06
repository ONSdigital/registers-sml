package uk.gov.ons.registers.helpers

object EitherSupport {
  def fromEithers[A, B, C, E](ea: Either[E, A], eb: Either[E, B])
   (onFailure: List[E] => C, onSuccess: (A, B) => C): C = (ea, eb) match {
    case(Right(ra), Right(rb)) => onSuccess(ra, rb)
    case(Left(exA), Left(exB)) => onFailure(List(exA, exB))
    case(Left(ex), _) => onFailure(List(ex))
    case(_, Left(ex)) => onFailure(List(ex))
  }
}
