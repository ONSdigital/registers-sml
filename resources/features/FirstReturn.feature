Feature: Test First Return

  Scenario Outline: Identify First Returns
    Given the user provides the file paths:
      | input_data_path   | expected_data_path   |
      | <input_data_path> | <expected_data_path> |
    And the user provides the partition,order and new column name:
      | partition_cols   | order_cols   | new_col_name   |
      | <partition_cols> | <order_cols> | <new_col_name> |
    When we apply the <Language> identify first returns function
    Then the <Language> first return should be marked as "1",otherwise "0" as in data from the file location expected_data_path

  @JVM
    Examples:
      | Language | input_data_path                        | partition_cols | order_cols | new_col_name     | expected_data_path                               |
      | Scala    | ./resources/inputs/FirstReturn_In.json | id             | period     | turnover_fr_flag | ./resources/outputs/FirstReturnSc1_Expected.json |

  Scenario Outline: Identify Large First Returns
    Given the user provides the file paths:
      | input_data_path   | expected_data_path   |
      | <input_data_path> | <expected_data_path> |
    And the user provides the threshold percentage,which item's firstreturn to be flagged up, partition and new columns:
      | partition_cols   | which_item_fr   | new_col_name   | threshold_percentage   |
      | <partition_cols> | <which_item_fr> | <new_col_name> | <threshold_percentage> |
    When we apply the <Language> large first returns function
    Then the <Language> large first return should be marked as "2" as in in data from the file location expected_data_path

  @JVM
    Examples:
      | Language | input_data_path                                  | partition_cols | which_item_fr | new_col_name     | threshold_percentage | expected_data_path                               |
      | Scala    | ./resources/outputs/FirstReturnSc1_Expected.json | period         | turnover      | turnover_fr_flag | 5                    | ./resources/outputs/FirstReturnSc2_Expected.json |


Scenario Outline: Identify First Returns and Large First Returns

    Given the user provides the file paths:
      | input_data_path   | expected_data_path   |
      | <input_data_path> | <expected_data_path> |
    And the user provides column names for the parameters partition,order,new column,which items first return and threshold percentage:
      | partition_cols   | order_cols   | new_col_name   | which_item_fr   | threshold_percentage   |
      | <partition_cols> | <order_cols> | <new_col_name> | <which_item_fr> | <threshold_percentage> |
    When we apply the <Language> first returns function to identify first return and large first return
    Then the <Language> first return should be marked as "1",large first returns as "2",otherwise "0" as in data from the file location expected_data_path

  @JVM
    Examples:
      | Language | input_data_path                        | partition_cols | order_cols | new_col_name     | which_item_fr | threshold_percentage | expected_data_path                               |
      | Scala    | ./resources/inputs/FirstReturn_In.json | id             | period     | turnover_fr_flag | turnover      | 5                 | ./resources/outputs/FirstReturnSc2_Expected.json |
      | Java     | ./resources/inputs/FirstReturn_In.json | id             | period     | turnover_fr_flag | turnover      | 5                 | ./resources/outputs/FirstReturnSc2_Expected.json |

  @Py
    Examples:
      | Language | input_data_path                        | partition_cols | order_cols | new_col_name     | which_item_fr | threshold_percentage | expected_data_path                               |
      | Python   | ./resources/inputs/FirstReturn_In.json | id             | period     | turnover_fr_flag | turnover      | 5                 | ./resources/outputs/FirstReturnSc2_Expected.json |
