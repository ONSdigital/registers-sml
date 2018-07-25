#' Duplicate Marker Function
#'
#' This Method marks the record with the highest order column value as the record to use amongst the
#'
#' @param sc A \code{spark_connection}.
#' @param data A \code{jobj}: the Spark \code{DataFrame} on which to perform the
#'   function.
#' @param partition_cols list(string). Column(s) on which to partition on
#' @param order_cols list(string), Columns(s) on which to order on
#' @param new_col_name c(string) The New Column Name created
#'
#' @return Returns a \code{jobj}
#'
#' @examples
#' \dontrun{
#' sc <- spark_coonect(master = "local", version = "2.2.0")
#' duplicate_data <- spark_read_json (
#'  sc,
#'  "duplicate_data",
#'  path = system.file(
#'       "data_raw/Duplicate.json",
#'       package = "sml"
#'       )
#'    ) %>%
#'      spark_dataframe()
#'
#'    # Call the method
#'    p <- sdf_melt (
#'    sc = sc, data = duplicate_data, partition_cols = c("id", "order")),
#'
#''#' # Return the data to R
#'p %>% dplyr::collect()
#'
#'spark_disconnect(sc = sc)
#' #
#' }
#'
#' @export
#'

sdf_duplicate <- function(sc,
                          data,
                          partition_cols,
                          order_cols,
                          new_col_name = "duplicate") {
  stopifnot(
    inherits(
      sc, c("spark_connection", "spark_shell_connection", "DBIConnection")
    )
  )
  stopifnot(inherits(data, c("spark_jobj", "shell_jobj")))
  stopifnot(is.character(partition_cols))
  stopifnot(is.character(order_cols))
  stopifnot(is.character(new_col_name))

  invoke_static(
    sc = sc,
    class = "uk.gov.ons.methods.Duplicate",
    method = "duplicate",
    df = data
  ) %>%
    invoke(
      method = "dm1",
      dfIn = data,
      partition_columns = scala_list(sc, partition_cols),
      order_columns = scala_list(sc, order_cols),
      new_column = new_col_name
    )
}

