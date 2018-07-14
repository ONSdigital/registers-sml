Feature: Making a Sample
    The sample creation method will ingest a frame and a set of stratification parameters to produce an output sample.
    To do this the pre-filtered dataset is sorted in ascending order by 'prn'. We then use the start point parameter
    (from the stratification properties file), and select units in prn order until the required sample size (number)
    is reached. The list wraps around, thus avoiding the issue of a index out of range when a big 'sample size' or a
    high 'start point' is given.

    Scenario Outline: Sample size parameter is within bounds of the sorted records
        Given a dataset and a stratification properties file containing sample size and start point parameter:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When the <language> sampling method is ran on the pre-filtered input
#        Then a DataFrame of given sample size is returned

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                             | output_path                                  |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/strat_properties.csv  | ./resources/outputs/sampling/sample_data     |
#        | Java     | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/strat_properties.csv  | ./resources/outputs/sampling/sample_data.csv |


#    Scenario Outline: Sample size parameter is out of bounds of the sorted records
#        Given a dataset and a stratification properties file:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When the <language> sampling method is ran on the pre-filtered input
#        Then a DataFrame of given sample size is returned






