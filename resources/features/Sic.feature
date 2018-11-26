Feature: A sic calculation groups the input data by substrings of SIC and ID to determine which subgroup contains the highest amount of employees
         and assigns a subdivision determined by the division and group.

         The method uses the following substrings:
         first 2 characters as division,
         first 3 characters as group,
         first 4 characters as class,
         and the whole whole SIC.

         Special cases decide subdivision:
         First if group equals 461, 478 or 479 then the subdivision is A
         Then if division equals 46 or 47 the subdivision is B
         Subdivision is C otherwise

    @Happy Path
    Scenario Outline: Happy Path - Origional Happy Path
        Given input:
        | ern | lurn | sic07 | employees |
        | 123 |    1 | 63111 |       504 |
        | 345 |    2 | 63110 |        46 |
        | 456 |    3 | 63120 |       540 |
        | 123 |    4 | 41202 |       844 |
        | 654 |    5 | 78109 |       517 |
        | 123 |    6 | 69201 |       188 |
        | 345 |    7 | 12312 |       100 |
        | 456 |    8 | 46123 |       500 |
        | 345 |    9 | 46212 |       300 |
        | 123 |   10 | 46444 |       301 |
        | 345 |   11 | 47544 |       500 |
        | 654 |   12 | 47144 |      1000 |
        | 123 |   13 | 47344 |       300 |
        | 123 |   14 | 47844 |       900 |
        | 123 |   15 | 47944 |       950 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern | lurn | sic07 | class | group | division | employees |
        | 456 |    3 | 63120 |  6312 |   631 |       63 |       540 |
        | 345 |   11 | 47544 |  4754 |   475 |       47 |       500 |
        | 654 |   12 | 47144 |  4714 |   471 |       47 |      1000 |
        | 123 |   15 | 47944 |  4794 |   479 |       47 |       950 |

    @JVM
    Examples:
    | language |
    | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - A Results table is which hits all endpoints of the decision tree
        Given input:
        | ern |       lurn | sic07 | employees |
        |   1 |  220000001 | 18201 |       100 |
        |   1 |  220000002 | 46579 |        78 |
        |   2 |  220000003 | 99808 |        80 |
        |   2 |  220000004 | 46120 |       120 |
        |   2 |  220000005 | 47123 |        20 |
        |   2 |  220000006 | 77777 |        10 |
        |   3 |  220000007 | 64738 |        99 |
        |   3 |  220000008 | 46345 |       125 |
        |   3 |  220000009 | 47000 |        45 |
        |   4 |  220000010 | 46888 |        33 |
        |   4 |  220000011 | 47898 |       100 |
        |   4 |  220000012 | 46999 |       125 |
        |   5 |  220000013 | 12323 |        35 |
        |   5 |  220000014 | 46765 |        55 |
        |   5 |  220000015 | 45564 |        67 |
        |   5 |  220000016 | 47190 |       135 |
        |   6 |  220000026 | 34226 |        12 |
        |   6 |  220000027 | 47555 |       140 |
        |   6 |  220000028 | 42222 |        66 |
        |   6 |  220000029 | 46756 |        44 |
        |   7 |  220000040 | 26354 |        55 |
        |   7 |  220000041 | 98763 |        20 |
        |   7 |  220000042 | 47876 |       140 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   1 | 220000001 | 18201 |  1820 |   182 |       18 |       100 |
        |   2 | 220000004 | 46120 |  4612 |   461 |       46 |       120 |
        |   3 | 220000008 | 46345 |  4634 |   463 |       46 |       125 |
        |   4 | 220000012 | 46999 |  4699 |   469 |       46 |       125 |
        |   5 | 220000016 | 47190 |  4719 |   471 |       47 |       135 |
        |   6 | 220000027 | 47555 |  4755 |   475 |       47 |       140 |
        |   7 | 220000042 | 47876 |  4787 |   478 |       47 |       140 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - A Results table is produced which combines the individual tests below
        Given input:
        | ern |       lurn | sic07 | employees |
        |   1 |  220000001 | 18201 |        50 |
        |   1 |  220000002 | 26110 |        20 |
        |   2 |  220000003 | 26120 |        20 |
        |   2 |  220000004 | 26400 |        30 |
        |   2 |  220000005 | 26120 |        20 |
        |   3 |  220000006 | 26400 |        25 |
        |   3 |  220000007 | 18201 |        25 |
        |   4 |  220000008 | 46210 |        20 |
        |   4 |  220000009 | 46310 |        15 |
        |   4 |  220000010 | 46610 |        25 |
        |   4 |  220000011 | 46900 |        50 |
        |   4 |  220000012 | 46110 |        40 |
        |   5 |  220000013 | 47190 |        35 |
        |   5 |  220000014 | 47210 |        10 |
        |   5 |  220000015 | 47300 |        20 |
        |   5 |  220000016 | 47110 |        35 |
        |   5 |  220000017 | 47810 |        30 |
        |   5 |  220000018 | 47910 |        10 |
        |   5 |  220000019 | 47990 |        40 |
        |   5 |  220000020 | 47220 |        30 |
        |   5 |  220000021 | 47220 |        20 |
        |   6 |  220000022 | 26120 |        20 |
        |   6 |  220000023 | 26400 |        30 |
        |   6 |  220000024 | 18201 |        10 |
        |   6 |  220000025 | 18201 |        10 |
        |   6 |  220000026 | 26120 |        20 |
        |   6 |  220000027 | 46210 |        40 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   1 | 220000001 | 18201 |  1820 |   182 |       18 |        50 |
        |   2 | 220000003 | 26120 |  2612 |   261 |       26 |        20 |
        |   3 | 220000007 | 18201 |  1820 |   182 |       18 |        25 |
        |   4 | 220000010 | 46610 |  4661 |   466 |       46 |        25 |
        |   5 | 220000020 | 47220 |  4722 |   472 |       47 |        30 |
        |   6 | 220000022 | 26120 |  2612 |   261 |       26 |        20 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - A - Simple Split (no SIC aggregation)
        Given input:
        | ern |       lurn | sic07 | employees |
        |   1 |  220000001 | 18201 |        50 |
        |   1 |  220000002 | 26110 |        20 |
            When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   1 | 220000001 | 18201 |  1820 |   182 |       18 |        50 |

    @Happy Path
    Scenario Outline: Happy Path - B - Simple Split (SIC aggregation)
        Given input:
        | ern |       lurn | sic07 | employees |
        |   2 |  220000004 | 26400 |        30 |
        |   2 |  220000005 | 26120 |        20 |
        |   2 |  220000003 | 26120 |        20 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   2 | 220000003 | 26120 |  2612 |   261 |       26 |        20 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - C - Simple Split (Employees Tied)
        Given input:
        | ern |       lurn | sic07 | employees |
        |   3 |  220000006 | 26400 |        25 |
        |   3 |  220000007 | 18201 |        25 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   3 | 220000007 | 18201 |  1820 |   182 |       18 |        25 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - D - Complex Split - Div 46
        Given input:
        | ern |       lurn | sic07 | employees |
        |   4 |  220000008 | 46210 |        20 |
        |   4 |  220000009 | 46310 |        15 |
        |   4 |  220000010 | 46610 |        25 |
        |   4 |  220000011 | 46900 |        50 |
        |   4 |  220000012 | 46110 |        40 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   4 | 220000010 | 46610 |  4661 |   466 |       46 |        25 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - E - Complex Split - Div 47
        Given input:
        | ern |       lurn | sic07 | employees |
        |   5 |  220000013 | 47190 |        35 |
        |   5 |  220000014 | 47210 |        10 |
        |   5 |  220000015 | 47300 |        20 |
        |   5 |  220000016 | 47110 |        35 |
        |   5 |  220000017 | 47810 |        30 |
        |   5 |  220000018 | 47910 |        10 |
        |   5 |  220000019 | 47990 |        40 |
        |   5 |  220000020 | 47220 |        30 |
        |   5 |  220000021 | 47220 |        20 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   5 | 220000020 | 47220 |  4722 |   472 |       47 |        30 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Happy Path
    Scenario Outline: Happy Path - F - Simple Split (SIC Aggregation with Employees Tied)
        Given input:
        | ern |       lurn | sic07 | employees |
        |   6 |  220000022 | 26120 |        20 |
        |   6 |  220000023 | 26400 |        30 |
        |   6 |  220000024 | 18201 |        10 |
        |   6 |  220000025 | 18201 |        10 |
        |   6 |  220000026 | 26120 |        20 |
        |   6 |  220000027 | 46210 |        40 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern |      lurn | sic07 | class | group | division | employees |
        |   6 | 220000022 | 26120 |  2612 |   261 |       26 |        20 |

        @JVM
        Examples:
        | language |
        | Scala    |

    @Sad Path
    Scenario Outline: Sad Path - We have an invalid input field
        Given input:
        |  id | sic07 | Invalid |
        | 123 | 63111 |     504 |
        | 345 | 63110 |      46 |
        | 456 | 63120 |     540 |
        | 123 | 41202 |     844 |
        | 654 | 78109 |     517 |
        | 123 | 69201 |     188 |
        | 345 | 12312 |     100 |
        | 456 | 46123 |     500 |
        | 345 | 46212 |     300 |
        | 123 | 46444 |     301 |
        | 345 | 47544 |     500 |
        | 654 | 47144 |    1000 |
        | 123 | 47344 |     300 |
        | 123 | 47844 |     900 |
        | 123 | 47944 |     950 |
        When the Sic method is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to calculate Sic

    @JVM
    Examples:
    | language |
    | Scala    |





