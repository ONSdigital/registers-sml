package uk.gov.ons.methods.impl

case class ONSRuntimeException(cause: String) extends RuntimeException(cause)