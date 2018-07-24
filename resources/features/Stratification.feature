Feature: Create a Stratified Frame
    This feature takes an input Frame that reflects some statistical unit type data assigned with a prn and stratifies it.
    Stratas are defined in a provided properties file. For every given strata (without regard for the selection type) a
    set of numerical range filters (e.g. payeEmployee, sic07) are imposed on the Frame. A produced stratified cell is
    then allocated a cell number (as per strata) which is appended to each of its rows.

    Scenario Outline: Apply sic07 and payeEmployee range filters on a Unstratified Frame
        Given a Frame and a Stratification Properties file paths:
            | data_input_path   | strat_properties        | expected_output   |
            | <input_path>      | <strat_properties_path> | <output_path>     |
        When a <language> Stratfiied Frame is created from the pre-filtered Frame
        Then a Stratfiied Frame for all given stratas is returned and exported to CSV

        @JVM
        Examples:
        | language | input_path                                       | strat_properties_path                          | output_path                                       |
        | Scala    | ./resources/inputs/stratification/frame_data.csv | ./resources/inputs/stratification/stratas.csv  | ./resources/outputs/stratification/sicAndEmployee |