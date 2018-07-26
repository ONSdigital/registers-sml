Feature: Creating a Sample
    The Sample creation method will ingest a Frame and a set of Stratification Parameters to produce an output Sample.
    Each of the stratification stratas will have a selection type assigned (Universe, Census, PRN). Only those strata 
    marked as ‘Census’ or ‘PRN Sampling’ are then processed. In the case of PRN Sampling, the strata is sorted by ‘prn’ 
    as a decimal type in ascending order; the prn start point from the selection parameters is identified and units are 
    ‘selected’ in prn order until the number required is achieved. Census strata require all units in the strata to be 
    returned. The selections for each strata are output collectively in a single DataFrame with the strata ID (cell 
    number) appended to each record based on which strata it belongs to.

    Scenario Outline: A strata for Census is created
        Given a Stratified Frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample DataFrame containing the Census strata is returned and exported to CSV

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                         | output_path                             |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/census.csv        | ./resources/outputs/sampling/census     |


    Scenario Outline: A strata for Prn-Sampling is created
        Given a Stratified Frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample DataFrame containing the Prn-Sampling strata is returned and exported to CSV

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                         | output_path                               |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/prn_sampling.csv  | ./resources/outputs/sampling/prn_sampling |


    Scenario Outline: Stratas contain Sample Size parameter that are out of bounds of the sorted Frame records.
        Given a Stratified Frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample DataFrame is returned and exported to CSV with the inclusion of stratas with outbound Sample Size parameter

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                          | output_path                                         |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/out_bounds_prn.csv | ./resources/outputs/sampling/sample_data/out_bounds |

#    Scenario Outline: Frame file cannot be found in given Frame directory
#        Given a Stratified Frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a file not found Exception in <language> is thrown for Frame file upon trying to Sample
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/invalid.csv    | ./resources/inputs/sampling/strat_properties_short.csv  | ./resources/outputs/sampling/sample_data/in_bounds  |


#    Scenario Outline: Stratification properties file cannot be found in given directory and an exception is thrown
#        Given a Stratified Frame and a stratification properties file paths:
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
#        Given a Stratified Frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a <language> Sample is created from the pre-filtered Frame
#        Then a Sample DataFrame is returned and exported to CSV, with the invalid Prn Start Point strata logged
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                   | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/invalid_prn_start_point.csv | ./resources/outputs/sampling/sample_data/in_bounds  |

    Scenario Outline: A Sample Size greater than the Frame length is given in the stratification properties
        Given a Stratified Frame and a stratification properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample DataFrame is returned and exported to CSV with the invalid Sample Size strata logged and entire Frame returned for that strata

        @JVM
        Examples:
        | language | input_path                                 | strat_properties_path                                 | output_path                                       |
        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/sample_size_too_great.csv | ./resources/outputs/sampling/sample_size_greater  |

#    Scenario Outline: A Sample Size less than 0 is given in the stratification properties
#        Given a Stratified Frame and a stratification properties file paths:
#            | data_input_path   | strat_properties        | expected_output   |
#            | <input_path>      | <strat_properties_path> | <output_path>     |
#        When a <language> Sample is created from the pre-filtered Frame
#        Then a Sample DataFrame is returned and exported to CSV, with the invalid Sample Size strata logged
#
#        @JVM
#        Examples:
#        | language | input_path                                 | strat_properties_path                                 | output_path                                         |
#        | Scala    | ./resources/inputs/sampling/frame_data.csv | ./resources/inputs/sampling/invalid_sample_size.csv   | ./resources/outputs/sampling/sample_data/in_bounds  |


