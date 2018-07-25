spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        "java/sml-1.0-SNAPSHOT-jar-with-dependencies.jar",
        package = "sml"
      )
    ),
    packages = c()
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
