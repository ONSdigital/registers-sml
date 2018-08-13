package uk.gov.ons.registers.support

import java.io.File
import java.nio.file.{Files, Path}

object TestFileEnvSetup {
  def createTempFile(prefix: String, suffix: String = "_test.csv"): Path = {
    val testPath = Files.createTempFile(prefix, suffix)
    // TODO - ADD test logger
    println(s"[INFO] Temporary file [${testPath.getFileName}] created at path: $testPath")
    testPath
  }

  def createTempDirectory(prefix: String): Path = {
    val testDir = Files.createTempDirectory(prefix)
    // TODO - ADD test logger
    println(s"[INFO] Temporary directory [${testDir.getFileName}] created at parent path: $testDir")
    testDir
  }

  def createAPath(pathStr: String): Path =
    new File(pathStr).toPath
}
