Feature: Imputed turnover is the employees multiplied by the TPH (as INT)
         Imputed employes is turnover divided by the TPH (as INT)

    @HappyPath
    Scenario Outline: Happy Path - An Imputed results table is calculated with all scenarios covered
        Given an employees and turnover input:
        | ern     | sic07 | turnover | paye_empees |
        | 1000001 |  2005 |       12 |          10 |
        | 1000002 |  2005 |     null |          12 |
        | 1000003 |  2005 |       10 |        null |
        | 1000004 |  2005 |     null |        null |
        And a TPH input:
        | sic07 |  TPH |
        |  2005 |    2 |
        When the Imputor is run
        Then the Imputed results table is produced:
        | ern     | imp_turnover | imp_empees |
        | 1000001 |           20 |          6 |
        | 1000002 |           24 |       null |
        | 1000003 |         null |          5 |
        | 1000004 |         null |       null |

        @JVM
        Examples:
        | language |
        | Scala    |

    @SadPath
    Scenario Outline: Sad Path - Employees and turnover input has invalid field
        Given an employees and turnover input:
        | ern     | sic07 | turnover | INVALID |
        | 1000001 |  2005 |       12 |      10 |
        | 1000002 |  2005 |     null |      10 |
        | 1000003 |  2005 |       12 |    null |
        | 1000004 |  2006 |       12 |    null |
        And a TPH input:
        | sic07 |  TPH |
        |  2005 |    2 |
        |  2006 | null |
        When the Imputor is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Impute

        @JVM
        Examples:
        | language |
        | Scala    |

    @SadPath
    Scenario Outline: Sad Path - TPH input has invalid field
        Given an employees and turnover input:
        | ern     | sic07 | turnover | paye_empees |
        | 1000001 |  2005 |       12 |          10 |
        | 1000002 |  2005 |     null |          10 |
        | 1000003 |  2005 |       12 |        null |
        | 1000004 |  2006 |       12 |        null |
        And an invalid TPH input:
        | sic07 |  invalid |
        |  2005 |        2 |
        |  2006 |     null |
        When the Imputor is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Impute

        @JVM
        Examples:
        | language |
        | Scala    |



