#' Call the Makring Between Limits function
#'
#' This method will .....
#'
#' @param sc A \code{spark_connection}.
#' @param data A \code{jobj}: the Spark \code{DataFrame} on which to perform the
#'   function.
#' @param value_col string. The column which will be ratio.
#' @param upper_limit int. The upper limit in which to compare to.
#' @param lower_limit int. The lower limit in which to compare to.
#' @param partition_cols list(string). A list of columns which the data will be paritioned on when lagging back.
#' @param order_cols list(string). A list of columns which the data will be ordered by when lagging back.
#' @param new_col_name string. The name of the new column created
#'
#' @return Returns a \code{jobj}
#'
#' @examples
#' \dontrun{
#' # Set up a spark connection
#' sc <- spark_connect(master = "local", version = "2.2.0")
#'
#' # Extract some data
#' mbl_data <- spark_read_json(
#'   sc,
#'   "mbl_data",
#'   path = system.file(
#'     "data_raw/mbl.json",
#'     package = "sparkts"
#'   )
#' ) %>%
#'   spark_dataframe()
#'
#' # Call the method
#' p <- sdf_marking_between_limits(
#'   sc = sc, data = mbl_data, value_col="value",
#'   upper_limit = 0.00135,lower_limit=0.0065,partition_cols="id",
#'   order_cols = "date", new_col_name = "marker"
#' )
#'
#' #' # Return the data to R
#' p %>% dplyr::collect()
#'
#' spark_disconnect(sc = sc)
#' }
#'
#' @export

sdf_marking_between_limits <- function(sc, data, value_col, upper_limit, lower_limit, partition_cols, order_cols, new_col_name) {
    stopifnot(
      inherits(
        sc, c("spark_connection", "spark_shell_connection", "DBIConnection")
        )
      )
    stopifnot(inherits(data, c("spark_jobj", "shell_jobj")))
    stopifnot(is.character(value_col), length(value_col) == 1)
    stopifnot(is.double(upper_limit), length(upper_limit) == 1)
    stopifnot(is.double(lower_limit), length(lower_limit) == 1)
    stopifnot(is.character(partition_cols))
    stopifnot(is.character(order_cols))
    stopifnot(is.character(new_col_name), length(new_col_name) == 1)

    invoke_static(
    sc = sc,
    class = "uk.gov.ons.methods.MarkingBetweenLimits",
    method = "markingBetweenLimits",
    df = data
    ) %>%
    invoke(
    method = "markingBetweenLimitsMethod",
    dfIn = data,
    valueColumnName = value_col,
    upperLimit = upper_limit,
    lowerLimit = lower_limit,
    partitionColumns = scala_seq(sc, partition_cols),
    orderColumns = scala_seq(sc, order_cols),
    newColumnName = new_col_name
    )
}
