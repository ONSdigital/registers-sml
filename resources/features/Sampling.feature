Feature: Making a Sample
    The sample creation method will ingest a frame and a set of stratification parameters to produce an output sample.
    To do this, the pre-filtered dataset is sorted in ascending order by 'prn'. Each of the stratification stratas with
    a selection type parameter set as 'Census' or 'Prn Sampling' (ommitting 'Universal') is then processed. We then use
    the start point parameter (of a given strata), and select units in prn order until the required sample size is
    reached. All samples are outputted collectively as a DataFrame, with an appended 'Cell Number' to each to indicate
    the strata to which it belongs.

    Scenario Outline: A strata for Census is created
        Given a frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from the pre-filtered frame
        Then a Sample DataFrame containing the Census strata is returned and exported to CSV

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                         | output_path                             |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/census.csv        | ./resources/outputs/sampling/census     |


    Scenario Outline: A strata for Prn-Sampling is created
        Given a frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from the pre-filtered frame
        Then a Sample DataFrame containing the Prn-Sampling strata is returned and exported to CSV

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                         | output_path                               |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/prn_sampling.csv  | ./resources/outputs/sampling/prn_sampling |


    Scenario Outline: Stratas contain Sample Size parameter that are out of bounds of the sorted frame records.
        Given a dataset and a stratification properties file:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from the pre-filtered frame
        Then a Sample DataFrame is returned and exported to CSV with the inclusion of stratas with outbound Sample Size parameter

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                          | output_path                                         |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/out_bounds_prn.csv | ./resources/outputs/sampling/sample_data/out_bounds |

#    Scenario Outline: Frame file cannot be found in given frame directory
#        Given a frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a file not found Exception in <language> is thrown for frame file upon trying to Sample
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/invalid.csv    | ./resources/inputs/sampling/strat_properties_short.csv  | ./resources/outputs/sampling/sample_data/in_bounds  |

#    Scenario Outline: Stratification properties file cannot be found in given directory and an exception is thrown
#        Given a frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a stratification properties file cannot be found upon trying to Sample
#        Then file not found Exception in <language> is thrown
#
#        @JVM
#        Examples:
#        | language | input_path                                    | strat_properties_path                    | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/frame_data.csv    | ./resources/inputs/sampling/invalid.csv  | ./resources/outputs/sampling/sample_data/in_bounds  |

#    Scenario Outline: An invalid Prn Start Point parameter is given in the stratification properties
#        Given a frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a <language> Sample is created from the pre-filtered frame
#        Then a Sample DataFrame is returned and exported to CSV, with the invalid Prn Start Point strata logged
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/invalid_prn_start_point.csv | ./resources/outputs/sampling/sample_data/in_bounds  |

    Scenario Outline: A Sample Size greater than the frame length is given in the stratification properties
        Given a frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from the pre-filtered frame
        Then a Sample DataFrame is returned and exported to CSV, with the invalid Sample Size strata logged and entire frame returned for that strata

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                                 | output_path                                       |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/sample_size_too_great.csv | ./resources/outputs/sampling/sample_size_greater  |

#    Scenario Outline: A Sample Size less than 0 is given in the stratification properties
#        Given a frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a <language> Sample is created from the pre-filtered frame
#        Then a Sample DataFrame is returned and exported to CSV, with the invalid Sample Size strata logged
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                 | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/invalid_sample_size.csv   | ./resources/outputs/sampling/sample_data/in_bounds  |


