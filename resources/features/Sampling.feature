Feature: Making a Sample
    The sample creation method will ingest a frame and a set of stratification parameters to produce an output sample.
    To do this, the pre-filtered dataset is sorted in ascending order by 'prn'. Each of the stratification requests with
    a selection type parameter set as 'Census' or 'PrnSampling' (ommitting 'Universal') is then processed. We then use
    the start point parameter (of each request), and select units in prn order until the required sample size (number)
    is reached. All requests are outputted collectively as a DataFrame, with an appended 'Cell Number' to each to
    indicate the source of request.

    Scenario Outline: Sample size parameter is within bounds of the sorted records
        Given a dataset and a stratification properties file containing sample size and start point parameter:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When the <language> sampling method is ran on the pre-filtered input
        Then a DataFrame of given sample size is returned

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/strat_properties_short.csv  | ./resources/outputs/sampling/sample_data/in_bounds  |


    Scenario Outline: Sample size parameter is out of bounds of the sorted records. The capturing of records will
    iterate back to the top of the DataFrame until the expected 'sample size' is reached.
        Given a dataset and a stratification properties file:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When the <language> sampling method is ran on the pre-filtered input
        Then a DataFrame of given sample size is returned

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/strat_properties_short.csv  | ./resources/outputs/sampling/sample_data/out_bounds |






