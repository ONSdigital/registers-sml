context("Test the sdf_first_return function")
sc <- testthat_spark_connection()

describe("Identify First Returns and Large Fisrt Returns", {
  it("Call sdf_first_return() function and Check actual outcome with expected data", {
    fr_in <- read_data(file.path(PROJHOME, "../resources/inputs/FirstReturn_In.json"))

    fr_expected <- read_data(file.path(PROJHOME, "../resources/outputs/FirstReturnSc2_Expected.json")) %>%
                                  dplyr::collect() %>%
                                  dplyr::select(id, period, turnover, turnover_fr_flag) %>%
                                  dplyr::mutate(turnover_fr_flag = as.integer(turnover_fr_flag)) %>%
                                  dplyr::arrange(period, id, turnover_fr_flag)

    fr_actual <- sdf_first_return(
      sc = sc, data = fr_in, part_col = c("id"), order_col = c("period"), new_col = "turnover_fr_flag",
      threshold_percentage = 5.0, which_item_fr = "turnover"
    ) %>%
      dplyr::collect() %>%
      dplyr::select(id, period, turnover, turnover_fr_flag) %>%
      dplyr::arrange(period, id, turnover_fr_flag)
    # Send the method output and expected output to a file
    tmp_sink_file_name <- tempfile(fileext = ".txt")
    tmp_sink_file_num <- file(tmp_sink_file_name, open = "wt")
    send_output(
      file = tmp_sink_file_name,
      name = "sdf_first_return_error",
      output = fr_actual,
      expected = fr_expected
    )
    close(tmp_sink_file_num)

    # Test the expectation
    tryCatch({
      expect_identical(
        fr_actual,
        fr_expected
      )
    },
    error = function(e) {
      cat("\n   Output data can be seen in ", tmp_sink_file_name, "\n", sep = "")
    }
    )
    expect_identical(
      fr_actual,
      fr_expected
    )

  })

})
