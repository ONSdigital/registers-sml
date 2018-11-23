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
    Scenario Outline: Happy Path - A Results table is produced
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
    Scenario Outline: Happy Path - A Results table is produced
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
    Scenario Outline: Happy Path - A Results table is produced
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
    Scenario Outline: Happy Path - A Results table is produced
        Given input:
        | ern | lurn | sic07 | employees |
        | 123 |    1 | 63111 |       504 |
        | 345 |    2 | 63110 |        46 |
        | 456 |    3 | 63120 |       540 |
        | 456 |    8 | 63120 |       530 |
        | 123 |    4 | 41202 |       844 |
        | 654 |    5 | 78109 |       517 |
        | 123 |    6 | 69201 |       188 |
        | 345 |    7 | 12312 |       100 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern | lurn | sic07 | class | group | division | employees |
        | 456 |    3 | 63120 |  6312 |   631 |       63 |       540 |
        | 123 |    4 | 41202 |  4120 |   412 |       41 |       844 |
        | 654 |    5 | 78109 |  7810 |   781 |       78 |       517 |
        | 345 |    7 | 12312 |  1231 |   123 |       12 |       100 |

        @JVM
        Examples:
        | language |
        | Scala    |


    @Happy Path
    Scenario Outline: Happy Path - A Results table is produced
        Given input:
        | ern | lurn | sic07 | employees |
        | 123 |  214 | 47190 |        35 |
        | 123 |  542 | 47210 |        10 |
        | 123 |  765 | 47300 |        20 |
        | 123 |  865 | 47110 |        35 |
        | 123 |  567 | 47810 |        30 |
        | 123 |  464 | 47910 |        10 |
        | 123 |  976 | 47990 |        40 |
        | 123 |  507 | 47220 |        30 |
        | 123 |  564 | 47220 |        20 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern | lurn | sic07 | class | group | division | employees |
        | 123 |  507 | 47220 |  4722 |   472 |       47 |        30 |


    @JVM
    Examples:
    | language |
    | Scala    |


    @Happy Path
    Scenario Outline: Happy Path - A Results table is produced
        Given input:
        | ern | lurn | sic07 | employees |
        |	1 | 5678 | 46210 |	      20 |
        |	1 | 6789 | 46310 |        15 |
        |	1 | 8907 | 46610 |        25 |
        |	1 | 2345 | 46900 |        50 |
        |	1 | 6443 | 46110 |        40 |
        |	1 | 2567 | 46609 |        25 |
        |	1 | 7454 | 46611 |        15 |
        |	1 | 9876 | 46612 |        10 |
        |	1 | 5646 | 46613 |        12 |

        When the Sic method is calculated
        Then the Sic results table is produced:
        | ern | lurn | sic07 | class | group | division | employees |
        |   1 | 2567 | 46609 |  4660 |   466 |       46 |        25 |


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





