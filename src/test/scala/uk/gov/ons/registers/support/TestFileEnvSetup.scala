package uk.gov.ons.registers.support

import java.io.File
import java.nio.file.{Files, Path}

import uk.gov.ons.registers.Patch

@deprecated
object TestFileEnvSetup {
  @deprecated
  def createTempFile(prefix: String, suffix: String = "_test.csv"): Path = {
    val testPath = Files.createTempFile(prefix, suffix)
    Patch.log(msg = s"Temporary file [${testPath.getFileName}] created at path: $testPath")
    testPath
  }

  @deprecated
  def createTempDirectory(prefix: String): Path = {
    val testDir = Files.createTempDirectory(prefix)
    Patch.log(msg = s"Temporary directory [${testDir.getFileName}] created at parent path: $testDir")
    testDir
  }

  @deprecated
  def createAPath(pathStr: String): Path =
    new File(pathStr).toPath
}
