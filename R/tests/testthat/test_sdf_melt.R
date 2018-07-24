context("Test the melt function")

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

describe("Melting (unpivoting) a DataFrame", {
  it("The variable name and value are both provided", {

    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/melt/BaseInput.json"))
    # Read in the expected data
    expected_data <-read_data(file.path(PROJHOME, "../resources/outputs/melt/BothProvided.json"))

    # Instantiate the class
    output <- sdf_melt(
      sc =  sc, data <- input_data, id_variables = c("identifier", "date"),
      value_variables <- c("one", "two", "three", "four"),
      variable_name <- "numberCol", value_name = "count")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, numberCol, count) %>%
      dplyr::arrange(identifier, date, count, numberCol)

    expected <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, numberCol, count) %>%
      dplyr::arrange(identifier, date, count, numberCol)

    compare_data(expected, output, "sdf_melt_s1")
  })

  it("The variable name and value name are not provided", {

    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/melt/BaseInput.json"))
    # Read in the expected data
    expected_data <-read_data(file.path(PROJHOME, "../resources/outputs/melt/NeitherProvided.json"))

    # Instantiate the class
    output <- sdf_melt(
      sc =  sc, data <- input_data, id_variables = c("identifier", "date"),
      value_variables <- c("one", "two", "three", "four"))

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, variable, value) %>%
      dplyr::arrange(identifier, date, value, variable)

    expected <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, variable, value) %>%
      dplyr::arrange(identifier, date, value, variable)

    compare_data(expected, output, "sdf_melt_s2")
  })

  it("The variable name is provided and the value name is not", {

    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/melt/BaseInput.json"))
    # Read in the expected data
    expected_data <-read_data(file.path(PROJHOME, "../resources/outputs/melt/VariableProvided.json"))

    # Instantiate the class
    output <- sdf_melt(
      sc =  sc, data <- input_data, id_variables = c("identifier", "date"),
      value_variables <- c("one", "two", "three", "four"),
      variable_name = "colNameProvided")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, colNameProvided, value) %>%
      dplyr::arrange(identifier, date, value, colNameProvided)

    expected <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, colNameProvided, value) %>%
      dplyr::arrange(identifier, date, value, colNameProvided)

    compare_data(expected, output, "sdf_melt_s3")
  })

  it("The value name is provided and the variable name is not", {

    # Read in the data
    input_data <-read_data(file.path(PROJHOME, "../resources/inputs/melt/BaseInput.json"))
    # Read in the expected data
    expected_data <-read_data(file.path(PROJHOME, "../resources/outputs/melt/ValueProvided.json"))

    # Instantiate the class
    output <- sdf_melt(
      sc =  sc, data <- input_data, id_variables = c("identifier", "date"),
      value_variables <- c("one", "two", "three", "four"),
      value_name = "valueColProvided")

    # Order and arrange the data for assertions
    output <- output %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, variable, valueColProvided) %>%
      dplyr::arrange(identifier, date, valueColProvided, variable)

    expected <- expected_data %>%
      dplyr::collect() %>%
      dplyr::select(identifier, date, variable, valueColProvided) %>%
      dplyr::arrange(identifier, date, valueColProvided, variable)

    compare_data(expected, output, "sdf_melt_s4")
  })

})

