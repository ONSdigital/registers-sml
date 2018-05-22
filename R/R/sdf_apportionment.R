#' Calling the Apportionment function.
#'
#' @param sc A \code{spark_connection}.
#' @param data_frame A \code{jobj}: the Spark \code{DataFrame} on which to perform the
#'   function.
#' @param aux_column String. Column used as a weighting.
#' @param date_column String. Column used to differentiate records of the same reference.
#' @param agg_column_a String. Column containing initial reference.
#' @param agg_column_b String. Column containing reference to apportion to.
#' @param app_columns c(String). Column(s) to be redistributed.
#'
#' @return Returns a \code{jobj}
#'
#' @examples
#' \dontrun{
#' # Set up a spark connection
#' sc <- spark_connect(master = "local", version = "2.2.0")
#'
#' # Read in the data
#' input_data <- sparklyr::spark_read_json(
#'   sc,
#'   "input_data",
#'   path = system.file(
#'     "..\\..\\resources\\inputs\\apportionment\\apportionmentInputSimple.json",
#'     package = "sml"
#'   )
#' ) %>%
#'   sparklyr::spark_dataframe()
#'
#' # Call the method
#' result_df <- sdf_apportionment(
#'   sc = sc,
#'   data_frame = input_data,
#'   aux_column = "ent_employment",
#'   date_column = "period",
#'   agg_column_a = "vatref9",
#'   agg_column_b = "ent_entref",
#'   app_columns = c("turnover")
#' )
#'
#' # Return the data to R
#' result_df %>% dplyr::collect()
#'
#' spark_disconnect(sc = sc)
#' }
#'
#' @export
sdf_apportionment <- function(sc, data_frame, aux_column, date_column, agg_column_a, agg_column_b, app_columns) {
  stopifnot(
    inherits(
      sc, c("spark_connection", "spark_shell_connection", "DBIConnection")
    )
  )
  stopifnot(inherits(data_frame, c("spark_jobj", "shell_jobj")))
  stopifnot(is.character(aux_column) & is.character(date_column) & is.character(agg_column_a) & is.character(agg_column_b) & is.vector(app_columns))

  invoke_static(
    sc = sc,
    class = "uk.gov.ons.methods.Apportionment",
    method = "apportionment",
    df = data_frame
  ) %>%
    invoke(
      method = "app1",
      auxColumn = aux_column,
      dateColumn = date_column,
      aggColumnA = agg_column_a,
      aggColumnB = agg_column_b,
      appColumns = scala_list(sc, app_columns)
    )
}
