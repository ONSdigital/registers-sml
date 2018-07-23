#' Call the First return
#'
#' This function add an extra column to flag up the first occurrence
#' and also large first occurrence of given item.
#'
#' @param sc A \code{spark_connection}.
#' @param data A \code{jobj}: the Spark \code{DataFrame} on which to perform the
#'   function.
#' @param part_col A list of Strings - column(s) names.
#' @param order_col A list of Strings - column(s) names.
#' @param new_col A string - new column name.
#' @param threshold_percentage A double - The first occurrence above this threshold should be flagged.
#' @param which_item_fr A string - which item first return have to be marked for example here it is turnover.
#'
#' @return Retruns a \code{jobj}
#' @export
#'
#' @examples
#' \dontrun{
#' # Set up a spark connection
#' sc <- spark_connect(master = "local", version = "2.2.0")
#'
#' # Extract some data
#' fr_data <- spark_read_json(
#'   sc,
#'   "fr_data",
#'   path =  "../../../resources/inputs/FirstReturn_In.json"
#' ) %>%
#'  spark_dataframe()
#'
#' # Call the method
#' out <- sml::sdf_first_return(
#'   sc = sc, data = fr_data, part_col = c("id"), order_col = c("period"), new_col = "turnover_fr_flag",
#'   threshold_percentage = 5.0, which_item_fr = "turnover"
#' )
#' #' # Return the data to R
#' out %>% dplyr::collect()
#'
#' # Close the spark connection
#'#' spark_disconnect(sc = sc)
#' }
#'
#' @export
sdf_first_return <- function(sc, data, part_col, order_col, new_col, threshold_percentage, which_item_fr ) {
  stopifnot(
    inherits(
      sc, c("spark_connection", "spark_shell_connection", "DBIConnection")
    )
  )
  stopifnot(inherits(data, c("spark_jobj", "shell_jobj")))
  stopifnot(is.character(part_col), length(part_col) == 1)
  stopifnot(is.character(order_col), length(order_col) == 1)
  stopifnot(is.character(new_col), length(new_col) == 1)
  stopifnot(is.numeric(threshold_percentage), length(threshold_percentage) == 1)
  stopifnot(is.character(which_item_fr), length(which_item_fr) == 1)

  invoke_static(
    sc = sc,
    class = "uk.gov.ons.methods.FirstReturn",
    method = "firstReturn",
    df = data
  ) %>%
    invoke(
      method = "firstReturn1",
      df = data,
      partitionColumns = scala_list(sc,part_col),
      orderColumns = scala_list(sc,order_col),
      newColName = new_col,
      thresholdPercentage = threshold_percentage,
      whichItemFr = which_item_fr
    )
}
