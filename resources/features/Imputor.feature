# new feature
# Tags: optional

Feature: Imputed turnover is the employees multiplied by the TPH (as INT)
         Imputed employes is turnover divided by the TPH (as INT)

    @HappyPath
<<<<<<< HEAD
    Scenario Outline: Happy Path - An Imputed results table is calculated with all scenarios covered
=======
    Scenario Outline: Happy Path - An Imputed results table is calculated
>>>>>>> 987654b80988501d7c88cd4e9beae7c899a0ed76
        Given an employees and turnover input:
        | ern     | sic07 | turnover | paye_empees |
        | 1000001 |  2005 |       12 |          10 |
        | 1000002 |  2005 |     null |          10 |
        | 1000003 |  2005 |       12 |        null |
<<<<<<< HEAD
        | 1000004 |  2005 |     null |        null |
        | 1000005 |  2006 |       12 |          10 |
=======
        | 1000004 |  2006 |       12 |        null |
>>>>>>> 987654b80988501d7c88cd4e9beae7c899a0ed76
        And a TPH input:
        | sic07 |  TPH |
        |  2005 |    2 |
        |  2006 | null |
        When the Imputor is run
        Then the Imputed results table is produced:
        | ern     | imp_turnover | imp_empees |
        | 1000001 |            6 |          5 |
        | 1000002 |         null |          5 |
        | 1000003 |            6 |       null |
        | 1000004 |         null |       null |
<<<<<<< HEAD
        | 1000005 |         null |       null |
=======
>>>>>>> 987654b80988501d7c88cd4e9beae7c899a0ed76

        @JVM
        Examples:
        | language |
        | Scala    |

    @SadPath
<<<<<<< HEAD
    Scenario Outline: Sad Path - Employees and turnover input has invalid field
        Given an employees and turnover input:
=======
    Scenario Outline: Sad Path - An Imputed results table is calculated
        Given an employees and invalid turnover input:
>>>>>>> 987654b80988501d7c88cd4e9beae7c899a0ed76
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
<<<<<<< HEAD
    Scenario Outline: Sad Path - TPH input has invalid field
=======
    Scenario Outline: Sad Path - An Imputed results table is calculated
>>>>>>> 987654b80988501d7c88cd4e9beae7c899a0ed76
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



