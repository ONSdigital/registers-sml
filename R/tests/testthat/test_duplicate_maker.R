context("Test Duplicate Marker function")

sc <- testthat_spark_connection()


compare_data <- function(expected, actual, name_of_file){
  # Test the expectation
  tryCatch({
    expect_identical(
      actual,
      expected
    )
  },
  error = function(e) {
    # Send the method output and expected output to a file
    tmp_sink_file_name <- tempfile(fileext = ".txt")
    tmp_sink_file_num <- file(tmp_sink_file_name, open = "wt")
    send_output(
      file = tmp_sink_file_name,
      name = name_of_file,
      output = actual,
      expected = expected
    )
    close(tmp_sink_file_num)
    cat("\n   Output data can be seen in ", tmp_sink_file_name, "\n", sep = "")
  }
  )
  expect_identical(
    actual,
    expected
  )
}

describe("Duplicate Marker", {
  it("Mark duplicate row based on partition and order column(s)", {

    # Expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/duplicate-marker/ROutputDataDuplicateMarker.json"))
    View(expected_data)
    # Read in data
    input_data <- read_data(file.path(PROJHOME, "../resources/inputs/duplicate-marker/InputDataDuplicateMarker.json"))
    View(input_data)

    # Instantiate the class
    output <- sdf_duplicate(
      sc = sc, data <- input_data,
      partition_cols <- c("num", "id"), order_cols = "order", new_col_name = "duplicate"
    )

    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, num, order, duplicate) %>%
      dplyr::mutate(duplicate = as.double(duplicate)) %>%
      dplyr::arrange(id, num, order, duplicate)
    expected <-expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, num, order, duplicate) %>%
      dplyr::arrange(id, num, order, duplicate)

    compare_data(expected, output, "sdf_duplicate_res")
  })
})

