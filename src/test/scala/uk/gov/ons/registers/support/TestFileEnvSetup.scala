package uk.gov.ons.registers.support

import java.nio.file.{Files, Path}

object TestFileEnvSetup {

  def createTempFile(prefix: String, suffix: String = "_test.csv"): Path = {
    val testPath = Files.createTempFile(prefix, suffix)
    // TODO - ADD test logger
    println(s"Temporary file [${testPath.getFileName}] created at path: $testPath")
    testPath
  }

  def createTempDirectory(prefix: String): Path = {
    val testDir = Files.createTempDirectory(prefix)
    // TODO - ADD test logger
    println(s"Temporary directory [${testDir.getFileName}] created at parent path: $testDir")
    testDir
  }
}
