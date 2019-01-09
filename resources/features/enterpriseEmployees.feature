@Cucumber.Options(features="src/test/resources")
Feature: Enterprise employees to be calculated by using either PAYE employees or imputed employees.
  If PAYE employees is non-null, Enterprise employees is PAYE employees.
  If PAYE employees is null and imputed employees is non-null, Enterprise employees is imputed employees.
  If both PAYE employees is null and imputed employees is null, Enterprise employees is 1.

  @HappyPath
  Scenario Outline: Happy Path - Employees is calculated
    Given an employees input:
      |       ern|paye_empees|imp_empees|
      |1100000001|          5|         6|
      |1100000002|          5|         7|
      |1100000003|       null|         7|
      |1100000004|       null|      null|
    When employees is calculated
    Then this Employees table is is produced
      |       ern|ent_empees|
      |1100000001|         5|
      |1100000002|         5|
      |1100000003|         7|
      |1100000004|         1|
  @JVM
  @George
    Examples:
      | language |
      | Scala    |
  @SadPath
  Scenario Outline: Sad Path - Employee Input has invalid field (employees)
    Given an employees input:
      |       ern|paye_empees|   INVALID|
      |1100000001|          5|         6|
      |1100000002|          5|         7|
      |1100000003|       null|         7|
      |1100000004|       null|      null|
    When the employees calculation is attempted
    Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate employees
  @JVM
  @George
    Examples:
      | language |
      | Scala    |