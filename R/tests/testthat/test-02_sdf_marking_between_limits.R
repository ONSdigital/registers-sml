context("Test the sdf_marking_between_limits function")

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


describe("Marking the ratio of values between two limits", {
  it("Ratio of Current and Previous value falls inbetween Limits", {

    # Read in the data
    input_data <- read_data(file.path(PROJHOME, "../resources/inputs/MarkingBetweenLimits/InsideLimitsData.json"))

    # Read in the expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/MarkingBetweenLimits/InsideLimitsOutput.json"))

    # Instantiate the class
    output <- sdf_marking_between_limits(
      sc =  sc, data = input_data, value_col= "value",
      upper_limit = 0.00135, lower_limit = 0.00065,
      partition_cols = c("id"), order_cols = c("date"), new_col_name = "marker")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::mutate(marker = as.numeric(marker)) %>%
      dplyr::arrange(id, date, value, marker)

    expected_data <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::arrange(id, date, value, marker)

    compare_data(expected_data, output, "sdf_marking_between_limits_s1")
  })

  it("Ratio of Current and Previous value is outside of Limits", {

    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/MarkingBetweenLimits/OutsideLimitsData.json"))

    # Read in the expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/MarkingBetweenLimits/OutsideLimitsOutput.json"))

    # Instantiate the class
    output <- sdf_marking_between_limits(
      sc =  sc, data = input_data, value_col= "value",
      upper_limit = 0.00135, lower_limit = 0.00065,
      partition_cols = c("id"), order_cols = c("date"), new_col_name = "marker")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::mutate(marker = as.numeric(marker)) %>%
      dplyr::arrange(id, date, value, marker)

    expected_data <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::arrange(id, date, value, marker)

    compare_data(expected_data, output, "sdf_marking_between_limits_s2")
  })

  it("No previous value", {
    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/MarkingBetweenLimits/NoPreviousData.json"))

    # Read in the expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/MarkingBetweenLimits/NoPreviousOutput.json"))

    # Instantiate the class
    output <- sdf_marking_between_limits(
      sc =  sc, data = input_data, value_col= "value",
      upper_limit = 0.00135, lower_limit = 0.00065,
      partition_cols = c("id"), order_cols = c("date"), new_col_name = "marker")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::mutate(marker = as.numeric(marker)) %>%
      dplyr::arrange(id, date, value, marker)

    expected_data <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::arrange(id, date, value, marker)

    compare_data(expected_data, output, "sdf_marking_between_limits_s3")
  })

  it("No current value", {
    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/MarkingBetweenLimits/NoCurrentData.json"))

    # Read in the expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/MarkingBetweenLimits/NoCurrentOutput.json"))

    # Instantiate the class
    output <- sdf_marking_between_limits(
      sc =  sc, data = input_data, value_col= "value",
      upper_limit = 0.00135, lower_limit = 0.00065,
      partition_cols = c("id"), order_cols = c("date"), new_col_name = "marker")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::mutate(marker = as.numeric(marker)) %>%
      dplyr::arrange(id, date, value, marker)

    expected_data <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::arrange(id, date, value, marker)

    compare_data(expected_data, output, "sdf_marking_between_limits_s4")
  })

  it("Previous value is zero", {
    # Read in the data
    input_data <- read_data(file.path(PROJHOME, "../resources/inputs/MarkingBetweenLimits/PreviousValueZeroData.json"))

    # Read in the expected data
    expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/MarkingBetweenLimits/PreviousValueZeroOutput.json"))

    # Instantiate the class
    output <- sdf_marking_between_limits(
      sc =  sc, data = input_data, value_col= "value",
      upper_limit = 0.00135, lower_limit = 0.00065,
      partition_cols = c("id"), order_cols = c("date"), new_col_name = "marker")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::mutate(marker = as.numeric(marker)) %>%
      dplyr::arrange(id, date, value, marker)

    expected_data <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(id, date, value, marker) %>%
      dplyr::arrange(id, date, value, marker)

    compare_data(expected_data, output, "sdf_marking_between_limits_s5")
  })

})

