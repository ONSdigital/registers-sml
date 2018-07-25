#' Connect to Spark
#'
#' This function defines the configurations for the Spark connection in Cloudera
#' Data Science Workbench.
#'
#' @return A \code{spark_connection}.
#'
#' @examples
#' \dontrun{
#' sc <- cloudera_spark_connect()
#' }
#'
#' @export
cloudera_spark_connect <- function() {
  config <- sparklyr::spark_config()
  config[["spark.r.command"]] <- "/usr/local/lib/R/bin/Rscript"
  config[["sparklyr.apply.env.R_HOME"]] <- "/usr/local/lib/R"
  config[["sparklyr.apply.env.R_SHARE_DIR"]] <- "/usr/local/lib/R/share"
  config[["sparklyr.apply.env.R_INCLUDE_DIR"]] <- "/usr/local/lib/R/include"

  sparklyr::spark_connect(master = "yarn-client", config = config)
}
