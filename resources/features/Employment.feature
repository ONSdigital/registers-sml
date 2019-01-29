Feature: Employment is calculated and is the sum of working propietors and employees.
         If Paye Employees is non-null, employment is Paye Employees + Working Proprietors.
         If Paye Employees is null then employment is Imputed Employees + Working Proprietors.
         If both Paye Employees and Imputed employees are null then employment is set to 1.
  @HappyPath
    Scenario Outline: Happy Path - Employment is calculated
    Given an employees and working proprietors input:
          |       ern| ent_empees|working_prop|
          |1100000001|          5|           9|
          |1100000002|          5|           8|
          |1100000003|          3|           8|
          |1100000004|          2|           8|
    When employment is calculated
    Then this Employment table is is produced
          |       ern|employment|
          |1100000001|        14|
          |1100000002|        13|
          |1100000003|        11|
          |1100000004|        10|
      @JVM
     Examples:
     | language |
     | Scala    |
  @SadPath
    Scenario Outline: Sad Path - Input has invalid field
    Given an employees and working proprietors input:
          |       ern| ent_empees|  INVALID|
          |1100000001|          5|        9|
          |1100000002|          5|        8|
          |1100000003|          3|        8|
          |1100000004|          1|        8|
    When the employment calculation is attempted
    Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate employment
      @JVM
     Examples:
     | language |
     | Scala    |