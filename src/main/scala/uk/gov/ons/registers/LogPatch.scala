package uk.gov.ons.registers

// NOTE: Patch until actual logging is set up
object LogPatch {
  def log(level: String = "info",  msg: String): Unit = println(s"[${level.capitalize}] $msg")
}