#Feature: Duplicate Marker Parameterized
#
#    Scenario Outline: Mark duplicate row based on partition and order column(s)
#        Given the user provides the file paths:
#          | input_dataset   | output_dataset    |
#          | <in_dataset>    | <out_dataset>     |
#        And the user provides the partition and order columns:
#          | partition_cols        | order_cols     |
#          | <partition_cols>      | <order_cols>   |
#        When the <language> Duplicate Marker function is applied
#        Then record with highest order column is marked with one to match expected data
#
#    @JVM
#    Examples:
#    | language  | in_dataset                                                            | partition_cols  | order_cols    | out_dataset                                                           |
#    | Scala     | ./resources/inputs/duplicate-marker/InputDataDuplicateMarker.json     | id,num          | order         | ./resources/outputs/duplicate-marker/OutputDataDuplicateMarker.json   |
#    | Java      | ./resources/inputs/duplicate-marker/InputDataDuplicateMarker.json     | id,num          | order         | ./resources/outputs/duplicate-marker/OutputDataDuplicateMarker.json   |
#
#    @Py
#    Examples:
#    | language  | in_dataset                                                            | partition_cols  | order_cols    | out_dataset                                                           |
#    | Python     | ./resources/inputs/duplicate-marker/InputDataDuplicateMarker.json    | id,num          | order         | ./resources/outputs/duplicate-marker/OutputDataDuplicateMarker.json   |
#    | Python     | ./resources/inputs/duplicate-marker/InputDataDuplicateMarker.json    | id,num          | order         | ./resources/outputs/duplicate-marker/OutputDataDuplicateMarker.json   |
