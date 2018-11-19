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
        |  id | sic07 | employees |
        | 123 | 63111 |       504 |
        | 345 | 63110 |        46 |
        | 456 | 63120 |       540 |
        | 123 | 41202 |       844 |
        | 654 | 78109 |       517 |
        | 123 | 69201 |       188 |
        | 345 | 12312 |       100 |
        | 456 | 46123 |       500 |
        | 345 | 46212 |       300 |
        | 123 | 46444 |       301 |
        | 345 | 47544 |       500 |
        | 654 | 47144 |     10000 |
        | 123 | 47344 |       300 |
        | 123 | 47844 |       900 |
        | 123 | 47944 |       950 |
        When the Sic method is calculated
        Then the Sic results table is produced:
        |  id | sic07 | class | group | division | subdivision | employees |
        | 123 | 47944 |  4794 |   479 |       47 |           A |       950 |
        | 345 | 47544 |  4754 |   475 |       47 |           B |       500 |
        | 456 | 63120 |  6312 |   631 |       63 |           C |       540 |
        | 654 | 47144 |  4714 |   471 |       47 |           B |      1000 |

    @JVM
    Examples:
    | language |
    | Scala    |

#  @Happy Path
#    Scenario Outline: Happy Path - A Results table is produced
#        Given input:
#        |  id | sic07 | employees |
#        | 123 | 63111 |       504 |
#        | 345 | 63110 |        46 |
#        | 456 | 63120 |       540 |
#        | 123 | 41202 |       844 |
#        | 654 | 78109 |       517 |
#        | 123 | 69201 |       188 |
#        | 345 | 12312 |       100 |
#
#        When the Sic method is calculated
#        Then the Sic results table is produced:
#        |  id | sic07 | employees |
#        | 345 | 12312 |       100 |
#        | 654 | 78109 |       517 |
#        | 456 | 63120 |       540 |
#        | 123 | 41202 |       844 |
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @Happy Path
#    Scenario Outline: Happy Path - A Results table is produced
#        Given input:
#        |  id | sic07 | employees |
#        | 123 | 47190 |        35 |
#        | 123 | 47210 |        10 |
#        | 123 | 47300 |        20 |
#        | 123 | 47110 |        35 |
#        | 123 | 47810 |        30 |
#        | 123 | 47910 |        10 |
#        | 123 | 47990 |        40 |
#        | 123 | 47220 |        30 |
#        | 123 | 47220 |        20 |
#        When the Sic method is calculated
#        Then the Sic results table is produced:
#        |  id | sic07 | employees |
#        | 123 | 47220 |        30 |
#
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#
#    @Happy Path
#    Scenario Outline: Happy Path - A Results table is produced
#        Given input:
#        |  id | sic07 | employees |
#        |	D | 46210 |	       20 |
#        |	D | 46310 |        15 |
#        |	D | 46610 |        25 |
#        |	D | 46900 |        50 |
#        |	D | 46110 |        40 |
#        |	D | 46609 |        25 |
#        |	D | 46611 |        15 |
#        |	D | 46612 |        10 |
#        |	D | 46613 |        12 |
#
#        When the Sic method is calculated
#        Then the Sic results table is produced:
#        |  id | sic07 | employees |
#        |   D | 46609 |        25 |
#
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @Happy Path
#    Scenario Outline: Happy Path - A Results table is produced
#        Given input:
#        |  id | sic07 | employees |
#        | 123 | 63111 |       504 |
#        | 345 | 63110 |        46 |
#        | 456 | 46123 |       500 |
#        | 456 | 46124 |       500 |
#        | 654 | 78109 |       517 |
#        When the Sic method is calculated
#        Then the Sic results table is produced:
#        |  id | sic07 | class | group | division | subdivision | employees |
#        | 123 | 63111 |  6311 |   631 |       63 |           C |       504 |
#        | 456 | 46124 |  4612 |   461 |       46 |           A |       500 |
#        | 456 | 46123 |  4612 |   461 |       46 |           A |       500 |
#        | 654 | 78109 |  7810 |   781 |       78 |           C |       517 |
#        | 345 | 63110 |  6311 |   631 |       63 |           C |        46 |
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#
#
#    @Sad Path
#    Scenario Outline: Sad Path - We have an invalid input field
#        Given input:
#        |  id | sic07 | Invalid |
#        | 123 | 63111 |     504 |
#        | 345 | 63110 |      46 |
#        | 456 | 63120 |     540 |
#        | 123 | 41202 |     844 |
#        | 654 | 78109 |     517 |
#        | 123 | 69201 |     188 |
#        | 345 | 12312 |     100 |
#        | 456 | 46123 |     500 |
#        | 345 | 46212 |     300 |
#        | 123 | 46444 |     301 |
#        | 345 | 47544 |     500 |
#        | 654 | 47144 |    1000 |
#        | 123 | 47344 |     300 |
#        | 123 | 47844 |     900 |
#        | 123 | 47944 |     950 |
#        When the Sic method is attempted
#        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to calculate Sic
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#
#
#
#
