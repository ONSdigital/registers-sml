Feature: Apportionment

    Scenario Outline: Simple Relationship

        Given there are datasets at the locations:
            | input_data        |
            | <input_data_path> |
        And the user provides the parameters:
            | aux       | date      | aggregate A   | aggregate B   | apportion     |
            | <aux_col> | <date_col>| <agg_col_A>   | <agg_col_B>   | <app_cols>    |
        And there is a simple relationship between two partitions
        When the <language> Apportionment function is applied to the dataset
        Then the Apportionment function will apportion the value to the output, matching an expected dataset at a location <expected_data_path>
        And the Apportionment function ensures that no turnover is lost in the output

        @JVM
        Examples: Apportionment parameters (JVM)
        | language  | input_data_path                                                   | expected_data_path                                                    | aux_col           | date_col  | agg_col_A     | agg_col_B     | app_cols   |
        | Scala     | ./resources/inputs/apportionment/apportionmentInputSimple.json    | ./resources/outputs/apportionment/apportionmentExpectedSimple.json    | ent_employment    | period    | vatref9       | ent_entref    | turnover   |
        | Java      | ./resources/inputs/apportionment/apportionmentInputSimple.json    | ./resources/outputs/apportionment/apportionmentExpectedSimple.json    | ent_employment    | period    | vatref9       | ent_entref    | turnover   |

        @Py
        Examples: Apportionment parameters (Py)
        | language  | input_data_path                                                   | expected_data_path                                                    | aux_col           | date_col  | agg_col_A     | agg_col_B     | app_cols   |
        | Python    | ./resources/inputs/apportionment/apportionmentInputSimple.json    | ./resources/outputs/apportionment/apportionmentExpectedSimple.json    | ent_employment    | period    | vatref9       | ent_entref    | turnover   |

    Scenario Outline: Complex Relationship

        Given there are datasets at the locations:
            | input_data        |
            | <input_data_path> |
        And the user provides the parameters:
            | aux       | date      | aggregate A   | aggregate B   | apportion     |
            | <aux_col> | <date_col>| <agg_col_A>   | <agg_col_B>   | <app_cols>    |
        And there is a complex relationship between two partitions
        When the <language> Apportionment function is applied to the dataset
        Then the Apportionment function will apportion the value to the output, matching an expected dataset at a location <expected_data_path>
        And the Apportionment function ensures that no turnover is lost in the output

        @JVM
        Examples: Apportionment parameters (JVM)
        | language  | input_data_path                                                   | expected_data_path                                                    | aux_col           | date_col  | agg_col_A     | agg_col_B     | app_cols   |
        | Scala     | ./resources/inputs/apportionment/apportionmentInputComplex.json   | ./resources/outputs/apportionment/apportionmentExpectedComplex.json   | ent_employment    | period    | vatref9       | ent_entref    | turnover   |
        | Java      | ./resources/inputs/apportionment/apportionmentInputComplex.json   | ./resources/outputs/apportionment/apportionmentExpectedComplex.json   | ent_employment    | period    | vatref9       | ent_entref    | turnover   |

        @Py
        Examples: Apportionment parameters (Py)
        | language  | input_data_path                                                   | expected_data_path                                                    | aux_col           | date_col  | agg_col_A     | agg_col_B     | app_cols   |
        | Python    | ./resources/inputs/apportionment/apportionmentInputComplex.json   | ./resources/outputs/apportionment/apportionmentExpectedComplex.json   | ent_employment    | period    | vatref9       | ent_entref    | turnover   |
