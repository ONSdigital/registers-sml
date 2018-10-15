# new feature
# Tags: optional
    
Feature: Employment is calculated and is the sum of working propietors and employees.

    @HappyPath
    Scenario Outline: A scenario
    Given some employees stuff amongst other stuff:
          |       ern|paye_empees|imp_empees|work_prop|
          |1100000001|          5|         6|        9|
          |1100000002|          5|         7|        8|
          |1100000003|       null|         7|        8|
          |1100000004|       null|      null|        8|
    When employment is calculated
    Then this Employment table is is produced
          |       ern|employment|
          |1100000001|        14|
          |1100000002|        13|
          |1100000003|        15|
          |1100000004|         1|

     @JVM
     Examples:
     | language |
     | Scala    |
