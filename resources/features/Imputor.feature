# new feature
# Tags: optional
    
Feature: Imputed turnover is the employees multiplied by the TPH (as INT)
         Imputed employes is turnover dibided by the TPH (as INT)
    
Scenario Outline: A scenario
    Given an employees and turnover input:
    | ern     | sic07   | turnover | paye_empees |
    | 1000001 | 2005    |       12 |          10 |
    And TPH input:
    | sic07   | TPH |
    | 2005    | 0.5 |
    When the Imputor is run
    Then the results are:
    | ern     | sic07   | turnover | paye_empees | imp_turnover | imp_empees |
    | 1000001 | 2005    |       12 |          10 |           24 |         20 |

    @JVM
    Examples:
    | language |
    | Scala    |
