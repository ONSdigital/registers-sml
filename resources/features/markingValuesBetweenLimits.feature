#Feature: Marking the ratio of values between two limits
#    This feature will look at specified column and divide its current value by the previous value. It will then
#	compare the output of this to specified upper and lower limits. If the output falls in between these limits,
#	then it gets flagged with a value of 1, whereas it it lies outside of these limits then it gets flagged with
#	a value of 0. These flags are stored in a extra column, where the name of it is specified.
#
#    Scenario Outline: Ratio of Current and Previous value falls inbetween Limits
#        Given the user provides the file paths:
#            | input_data	| expected_output	|
#        	| <input_path>  | <expected_path>	|
#		And the user provides the function parameters:
#		    | value_col   | upper_limit	  | lower_limit	  | partition_cols   | order_cols   | new_col_name   |
#            | <value_col> | <upper_limit> | <lower_limit> | <partition_cols> | <order_cols> | <new_col_name> |
#        When the <language> value divided by the previous value falls in between the prescribed upper and lower values
#        Then the <language> record will be flagged as 1
#
#        @JVM
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Scala    | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/InsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/InsideLimitsOutput.json |
#        | Java     | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/InsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/InsideLimitsOutput.json |
#
#        @Py
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Python   | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/InsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/InsideLimitsOutput.json |
#
#    Scenario Outline: Ratio of Current and Previous value is outside of Limits
#        Given the user provides the file paths:
#            | input_data	| expected_output	|
#        	| <input_path>  | <expected_path>	|
#		And the user provides the function parameters:
#		    | value_col   | upper_limit	  | lower_limit	  | partition_cols   | order_cols   | new_col_name   |
#            | <value_col> | <upper_limit> | <lower_limit> | <partition_cols> | <order_cols> | <new_col_name> |
#        When the <language> value divided by its previous is greater than the upper value or less than the lower limit
#        Then the <language> record will be flagged as 0
#
#        @JVM
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                    | expected_path                                                    |
#        | Scala    | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/OutsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/OutsideLimitsOutput.json |
#        | Java     | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/OutsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/OutsideLimitsOutput.json |
#
#        @Py
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Python   | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/OutsideLimitsData.json | ./resources/outputs/MarkingBetweenLimits/OutsideLimitsOutput.json |
#
#    Scenario Outline: No previous value
#        Given the user provides the file paths:
#            | input_data	| expected_output	|
#            | <input_path>  | <expected_path>	|
#    	And the user provides the function parameters:
#    	    | value_col   | upper_limit	  | lower_limit	  | partition_cols   | order_cols   | new_col_name   |
#            | <value_col> | <upper_limit> | <lower_limit> | <partition_cols> | <order_cols> | <new_col_name> |
#        When the <language> current value is divided by the previous value, where previous value is null
#        Then the <language> record will be flagged as 0
#
#        @JVM
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                 | expected_path                                                 |
#        | Scala    | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoPreviousData.json | ./resources/outputs/MarkingBetweenLimits/NoPreviousOutput.json |
#        | Java     | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoPreviousData.json | ./resources/outputs/MarkingBetweenLimits/NoPreviousOutput.json |
#
#        @Py
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Python   | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoPreviousData.json | ./resources/outputs/MarkingBetweenLimits/NoPreviousOutput.json |
#
#    Scenario Outline: No current value
#        Given the user provides the file paths:
#            | input_data	| expected_output	|
#            | <input_path>  | <expected_path>	|
#    	And the user provides the function parameters:
#    	    | value_col   | upper_limit	  | lower_limit	  | partition_cols   | order_cols   | new_col_name   |
#            | <value_col> | <upper_limit> | <lower_limit> | <partition_cols> | <order_cols> | <new_col_name> |
#        When there is no current value to perform the <language> calculation
#        Then the <language> record will be flagged as 0
#
#        @JVM
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                | expected_path                                                |
#        | Scala    | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoCurrentData.json | ./resources/outputs/MarkingBetweenLimits/NoCurrentOutput.json |
#        | Java     | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoCurrentData.json | ./resources/outputs/MarkingBetweenLimits/NoCurrentOutput.json |
#
#        @Py
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Python   | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/NoCurrentData.json | ./resources/outputs/MarkingBetweenLimits/NoCurrentOutput.json |
#
#    Scenario Outline: Previous value is zero
#        Given the user provides the file paths:
#            | input_data	| expected_output	|
#            | <input_path>  | <expected_path>	|
#        And the user provides the function parameters:
#            | value_col   | upper_limit	  | lower_limit	  | partition_cols   | order_cols   | new_col_name   |
#            | <value_col> | <upper_limit> | <lower_limit> | <partition_cols> | <order_cols> | <new_col_name> |
#        When the <language> current value is divided by the previous value, where previous value is zero
#        Then the <language> record will be flagged as 0
#
#        @JVM
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                        | expected_path                                                        |
#        | Scala    | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/PreviousValueZeroData.json | ./resources/outputs/MarkingBetweenLimits/PreviousValueZeroOutput.json |
#        | Java     | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/PreviousValueZeroData.json | ./resources/outputs/MarkingBetweenLimits/PreviousValueZeroOutput.json |
#
#        @Py
#        Examples:
#        | language | value_col | upper_limit | lower_limit | partition_cols | order_cols | new_col_name | input_path                                                   | expected_path                                                  |
#        | Python   | value     | 0.00135     | 0.00065     | id             | date       | marker       | ./resources/inputs/MarkingBetweenLimits/PreviousValueZeroData.json | ./resources/outputs/MarkingBetweenLimits/PreviousValueZeroOutput.json |
