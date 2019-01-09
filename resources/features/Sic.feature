Feature: The top down classification method considers units based on their SIC section letter (subdivision) then works down 2, 3, 4 and 5 digit
  level until a clear classification is determined.

  The method uses the following substrings of sic07:
  first 2 characters as division,
  first 3 characters as group,
  first 4 characters as class,
  and the whole whole SIC.

  It also uses subdivision which is determined by the division and the table can be viewed in confluence.
  https://collaborate2.ons.gov.uk/confluence/pages/viewpage.action?pageId=5386186

  The method works by drilling down these levels until it has a single sic07 for each enterprise which is output.

  There are special cases for divisions 46 and 47 which can also be viewed on the above confluence page.



  @Happy Path

  Scenario Outline: Happy Path - A Results table is which hits a few endpoints of the decision tree
    Given input:
      | ern | lurn      | sic07 | employees |
      | 1   | 220000001 | 99201 | 100       |
      | 1   | 220000002 | 02579 | 78        |
      | 2   | 220000003 | 99808 | 80        |
      | 2   | 220000004 | 46120 | 120       |
      | 2   | 220000005 | 47123 | 20        |
      | 2   | 220000006 | 77777 | 10        |
      | 3   | 220000007 | 64738 | 99        |
      | 3   | 220000008 | 46345 | 125       |
      | 3   | 220000009 | 47000 | 45        |
      | 4   | 220000010 | 46888 | 33        |
      | 4   | 220000011 | 47898 | 100       |
      | 4   | 220000012 | 46999 | 125       |
      | 5   | 220000013 | 12323 | 35        |
      | 5   | 220000014 | 46765 | 55        |
      | 5   | 220000015 | 45564 | 67        |
      | 5   | 220000016 | 47190 | 135       |
      | 6   | 220000026 | 35226 | 12        |
      | 6   | 220000027 | 47555 | 140       |
      | 6   | 220000028 | 42222 | 66        |
      | 6   | 220000029 | 46756 | 44        |
      | 7   | 220000040 | 26354 | 55        |
      | 7   | 220000041 | 35763 | 20        |
      | 7   | 220000042 | 47876 | 140       |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 7   | 47876 |
      | 3   | 46345 |
      | 5   | 47190 |
      | 6   | 47555 |
      | 1   | 99201 |
      | 4   | 46999 |
      | 2   | 46120 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - A - Simple Split (no SIC aggregation)
    Given input:
      | ern | lurn      | sic07 | employees |
      | 1   | 220000001 | 18201 | 50        |
      | 1   | 220000002 | 26110 | 20        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 1   | 18201 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - B - Simple Split (SIC aggregation)
    Given input:
      | ern | lurn      | sic07 | employees |
      | 2   | 220000004 | 26400 | 30        |
      | 2   | 220000005 | 26120 | 20        |
      | 2   | 220000003 | 26120 | 20        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 2   | 26120 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - C - Simple Split (Employees Tied)
    Given input:
      | ern | lurn      | sic07 | employees |
      | 3   | 220000006 | 26400 | 25        |
      | 3   | 220000007 | 18201 | 25        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 3   | 18201 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - D - Complex Split - Div 46
    Given input:
      | ern | lurn      | sic07 | employees |
      | 4   | 220000008 | 46210 | 20        |
      | 4   | 220000009 | 46310 | 15        |
      | 4   | 220000010 | 46610 | 25        |
      | 4   | 220000011 | 46900 | 50        |
      | 4   | 220000012 | 46110 | 40        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 4   | 46610 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - E - Complex Split - Div 47
    Given input:
      | ern | lurn      | sic07 | employees |
      | 5   | 220000013 | 47190 | 35        |
      | 5   | 220000014 | 47210 | 10        |
      | 5   | 220000015 | 47300 | 20        |
      | 5   | 220000016 | 47110 | 35        |
      | 5   | 220000017 | 47810 | 30        |
      | 5   | 220000018 | 47910 | 10        |
      | 5   | 220000019 | 47990 | 40        |
      | 5   | 220000020 | 47220 | 30        |
      | 5   | 220000021 | 47220 | 20        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 5   | 47220 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Happy Path

  Scenario Outline: Happy Path - F - Simple Split (SIC Aggregation with Employees Tied)
    Given input:
      | ern | lurn      | sic07 | employees |
      | 6   | 220000022 | 26120 | 20        |
      | 6   | 220000023 | 26400 | 30        |
      | 6   | 220000024 | 18201 | 10        |
      | 6   | 220000025 | 18201 | 10        |
      | 6   | 220000026 | 26120 | 20        |
      | 6   | 220000027 | 46210 | 40        |
    When the Sic method is calculated
    Then the Sic results table is produced:
      | ern | sic07 |
      | 6   | 26120 |

  @JVM
    Examples:
      | language |
      | Scala    |

  @Sad Path

  Scenario Outline: Sad Path - We have an invalid input field
    Given input:
      | ern | lurn     | sic07 | Invalid |
      | 123 | 22000001 | 63111 | 504     |
      | 123 | 22000001 | 69201 | 188     |
      | 123 | 22000001 | 46444 | 301     |
      | 123 | 22000001 | 41202 | 844     |
      | 123 | 22000001 | 47344 | 300     |
      | 123 | 22000001 | 47944 | 950     |
      | 123 | 22000001 | 47844 | 900     |
      | 345 | 22000001 | 63110 | 46      |
      | 345 | 22000001 | 12312 | 100     |
      | 456 | 22000001 | 46123 | 500     |
      | 345 | 22000001 | 46212 | 300     |
      | 345 | 22000001 | 47544 | 500     |
      | 456 | 22000001 | 63120 | 540     |
      | 654 | 22000001 | 47144 | 1000    |
      | 654 | 22000001 | 78109 | 517     |
    When the Sic method is attempted
    Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to calculate Sic

  @JVM
    Examples:
      | language |
      | Scala    |





