sc <- testthat_spark_connection()

test_that("Test the apportionment calculations are as expected for a simple relationship", {

  context("Test sdf_apportionment for simple relationship")

  # Read in the data
  input_data <- read_data(file.path(PROJHOME, "../resources/inputs/apportionment/apportionmentInputSimple.json"), sc)

  # Read in the expected data
  expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/apportionment/apportionmentExpectedSimple.json"), sc)

  # Call the method
  result_df <- sdf_apportionment(
    sc = sc,
    data_frame = input_data,
    aux_column = "ent_employment",
    date_column = "period",
    agg_column_a = "vatref9",
    agg_column_b = "ent_entref",
    app_columns = c("turnover")
  )

  # Assert result is equal to expected
  result_df <- result_df %>%
    dplyr::collect() %>%
    dplyr::select(vatref9, ent_entref, ent_employment, period, turnover) %>%
    dplyr::arrange(vatref9, ent_entref, period, turnover)
  expected_data <- expected_data %>%
    dplyr::collect() %>%
    dplyr::select(vatref9, ent_entref, ent_employment, period, turnover) %>%
    dplyr::arrange(vatref9, ent_entref, period, turnover)
  expect_identical(
    result_df,
    expected_data
  )
})

test_that("Test the apportionment calculations are as expected for a complex relationship", {

  context("Test sdf_apportionment for complex relationship")

  # Read in the data
  input_data <- read_data(file.path(PROJHOME, "../resources/inputs/apportionment/apportionmentInputComplex.json"), sc)

  # Read in the expected data
  expected_data <- read_data(file.path(PROJHOME, "../resources/outputs/apportionment/apportionmentExpectedComplex.json"), sc)

  # Call the method
  result_df <- sdf_apportionment(
    sc = sc,
    data_frame = input_data,
    aux_column = "ent_employment",
    date_column = "period",
    agg_column_a = "vatref9",
    agg_column_b = "ent_entref",
    app_columns = c("turnover")
  )

  # Assert result is equal to expected
  result_df <- result_df %>%
    dplyr::collect() %>%
    dplyr::select(vatref9, ent_entref, ent_employment, period, turnover) %>%
    dplyr::arrange(vatref9, ent_entref, period, turnover)
  expected_data <- expected_data %>%
    dplyr::collect() %>%
    dplyr::select(vatref9, ent_entref, ent_employment, period, turnover) %>%
    dplyr::arrange(vatref9, ent_entref, period, turnover)
  expect_identical(
    result_df,
    expected_data
  )
})
