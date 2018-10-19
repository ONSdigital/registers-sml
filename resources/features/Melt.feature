#Feature: Melt
#    The Melt method unpivots a DataFrame from wide format to long format, optionally leaving identifier variables set.
#    This function is useful to massage a DataFrame into a format where one or more columns are identifier variables (id_vars),
#    while all other columns, considered measured variables (value_vars), are 'unpivoted' to the row axis, leaving just
#    two non-identifier columns, 'variable' and 'value'
#
#    Scenario Outline: The variable name and value are both provided
#        Given the user provides the file paths:
#            | input_data   | expected_output |
#            | <input_path> | <expected_path> |
#        And the user provides the melt function parameters:
#            | id_vars   | value_vars    | var_name   | value_name   |
#            | <id_vars> |  <value_vars> | <var_name> | <value_name> |
#        When the <language> Melt function is applied
#        Then the DataFrame will be unpivoted correctly
#
#        @JVM
#        Examples:
#        | language | input_path                             | expected_path                              | id_vars         | value_vars         | var_name  | value_name |
#        | Scala    | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/BothProvided.json | identifier,date | one,two,three,four | numberCol | count      |
#        | Java     | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/BothProvided.json | identifier,date | one,two,three,four | numberCol | count      |
#
#        @Py
#        Examples:
#        | language | input_path                             | expected_path                              | id_vars         | value_vars         | var_name  | value_name |
#        | Python   | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/BothProvided.json | identifier,date | one,two,three,four | numberCol | count      |
#
#    Scenario Outline: The variable name and value name are not provided
#        Given the user provides the file paths:
#            | input_data   | expected_output |
#            | <input_path> | <expected_path> |
#        And the user provides the melt function parameters:
#            | id_vars   | value_vars    | var_name   | value_name   |
#            | <id_vars> |  <value_vars> | <var_name> | <value_name> |
#        When the <language> Melt function is applied
#        Then the DataFrame will be unpivoted correctly
#
#        @JVM
#        Examples:
#        | language | input_path                             | expected_path                                 | id_vars         | value_vars         | var_name  | value_name |
#        | Scala    | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/NeitherProvided.json | identifier,date | one,two,three,four | null      | null       |
#        | Java     | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/NeitherProvided.json | identifier,date | one,two,three,four | null      | null       |
#
#        @Py
#        Examples:
#        | language | input_path                             | expected_path                                 | id_vars         | value_vars         | var_name  | value_name |
#        | Python   | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/NeitherProvided.json | identifier,date | one,two,three,four | null      | null       |
#
#    Scenario Outline: The variable name is provided and the value name is not
#            Given the user provides the file paths:
#                | input_data   | expected_output |
#                | <input_path> | <expected_path> |
#            And the user provides the melt function parameters:
#                | id_vars   | value_vars    | var_name   | value_name   |
#                | <id_vars> |  <value_vars> | <var_name> | <value_name> |
#            When the <language> Melt function is applied
#            Then the DataFrame will be unpivoted correctly
#
#        @JVM
#        Examples:
#        | language | input_path                             | expected_path                                  | id_vars         | value_vars         | var_name        | value_name |
#        | Scala    | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/VariableProvided.json | identifier,date | one,two,three,four | colNameProvided | null       |
#        | Java     | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/VariableProvided.json | identifier,date | one,two,three,four | colNameProvided | null       |
#
#        @Py
#        Examples:
#        | language | input_path                             | expected_path                                  | id_vars         | value_vars         | var_name        | value_name |
#        | Python   | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/VariableProvided.json | identifier,date | one,two,three,four | colNameProvided | null       |
#
#    Scenario Outline: The value name is provided and the variable name is not
#            Given the user provides the file paths:
#                | input_data   | expected_output |
#                | <input_path> | <expected_path> |
#            And the user provides the melt function parameters:
#                | id_vars   | value_vars    | var_name   | value_name   |
#                | <id_vars> |  <value_vars> | <var_name> | <value_name> |
#            When the <language> Melt function is applied
#            Then the DataFrame will be unpivoted correctly
#
#        @JVM
#        Examples:
#        | language | input_path                             | expected_path                               | id_vars         | value_vars         | var_name | value_name       |
#        | Scala    | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/ValueProvided.json | identifier,date | one,two,three,four | null     | valueColProvided |
#        | Java     | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/ValueProvided.json | identifier,date | one,two,three,four | null     | valueColProvided |
#
#        @Py
#        Examples:
#        | language | input_path                             | expected_path                               | id_vars         | value_vars         | var_name | value_name       |
#        | Python   | ./resources/inputs/melt/BaseInput.json | ./resources/outputs/melt/ValueProvided.json | identifier,date | one,two,three,four | null     | valueColProvided |
