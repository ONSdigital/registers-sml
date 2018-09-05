Feature: PAYE Calculation
    The PAYE Calculation method will take information from the BI data and PAYE refs tables to calculate paye_employees and paye_jobs

    paye_employees takes sum of the average number of jobs per quarter of a payeref across a legal unit, so for each payeref sum the jobs for all the quaters and divide by the number of non-null quaters then sum these numbers

    paye_jobs takes the sum of the last quater of each payeref, so find the values of the last quater of each payeref and sum them

    Scenario Outline: Happy Path - with nulls
        Given the BI data input:
           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
           |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
           |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
           |BLACKWELLGROUP LT...|[1155L, 1154L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
           |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
           |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|

        And the PAYE refs input with nulls:
           |payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
           |  1151L|       1|        2|        3|       4|
           |  1152L|       5|        6|     null|       8|
           |  1153L|       9|        1|        2|       3|
           |  1154L|       4|     null|        6|       7|
           |  1155L|       8|        9|        1|       2|
           |  1166L|       1|        1|        2|       3|
           |  3333L|       1|        1|        2|       3|
           |  1188L|       2|        2|        2|       2|
           |  1199L|    null|     null|     null|    null|
           |  5555L|    null|     null|     null|    null|
           |  1999Z|       1|        3|        4|       5|
           |  9876L|       6|        5|        4|       5|
        When the method is applied
        Then a results table is produced:
           |paye_empees|paye_jobs|       ern|
           |          5|        5|2200000002|
           |         19|       20|1100000003|
           |          3|        5|9900000009|
           |          2|        4|2000000011|
           |          4|        8|1100000004|

    @JVM
    Examples:
    | language |
    | Scala    |

   Scenario Outline: Happy Path - without nulls
        Given the BI data input:
           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
           |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
           |BLACKWELLGROUP LT...|       [1153L]|               [111222333000]|1100000003|100000246017|
           |BLACKWELLGROUP LT...|       [1155L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
           |         IBM LTD - 2|       [1188L]|               [555666777002]|1100000004|100000508723|
           |         IBM LTD - 3|       [3333L]|               [999888777000]|1100000004|100000508724|
           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|

        And the PAYE refs input with nulls:
           | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
           |   1151L|       1|        2|        3|       4|
           |   1153L|       9|        1|        2|       3|
           |   1155L|       8|        9|        1|       2|
           |   1166L|       1|        1|        2|       3|
           |   3333L|       1|        1|        2|       3|
           |   1188L|       2|        2|        2|       2|
           |   1999Z|       1|        3|        4|       5|
           |   9876L|       6|        5|        4|       5|
        When the method is applied
        Then a results table is produced:
           |paye_empees|paye_jobs|       ern|
           |          5|        5|2200000002|
           |          8|        5|1100000003|
           |          3|        5|9900000009|
           |          2|        4|2000000011|
           |          4|        8|1100000004|

    @JVM
    Examples:
    | language |
    | Scala    |

    Scenario Outline: Sad Path - BI data input has invalid field
        Given a BI data input with field that does not exist:
           |        BusinessName|      PayeRefs|                      VatRefs|   Invalid|          id|
           |      INDUSTRIES LTD|         1152L|               [123123123000]|1100000002|100002826247|
           |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
           |BLACKWELLGROUP LT...|[1154L, 1155L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
           |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
           |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
        And the PAYE refs input:
            | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
            |   1151L|       1|        2|        3|       4|
            |   1152L|       5|        6|     null|       8|
            |   1153L|       9|        1|        2|       3|
            |   1154L|       4|     null|        6|       7|
            |   1155L|       8|        9|        1|       2|
            |   1166L|       1|        1|        2|       3|
            |   3333L|       1|        1|        2|       3|
            |   1188L|       2|        2|        2|       2|
            |   1199L|    null|     null|     null|    null|
            |   5555L|    null|     null|     null|    null|
            |   1999Z|       1|        3|        4|       5|
            |   9876L|       6|        5|        4|       5|
        When the method is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate PAYE

    @JVM
    Examples:
    | language |
    | Scala    |

    Scenario Outline: Sad Path - PAYE Refs input has invalid field
        Given the BI data input:
           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
           |      INDUSTRIES LTD|         1152L|               [123123123000]|1100000002|100002826247|
           |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
           |BLACKWELLGROUP LT...|[1154L, 1155L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
           |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
           |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
        And a PAYE refs input with invalid field:
            | payeref|mar_jobs|june_jobs|sept_jobs| INVALID|
            |   1151L|       1|        2|        3|       4|
            |   1152L|       5|        6|     null|       8|
            |   1153L|       9|        1|        2|       3|
            |   1154L|       4|     null|        6|       7|
            |   1155L|       8|        9|        1|       2|
            |   1166L|       1|        1|        2|       3|
            |   3333L|       1|        1|        2|       3|
            |   1188L|       2|        2|        2|       2|
            |   1199L|    null|     null|     null|    null|
            |   5555L|    null|     null|     null|    null|
            |   1999Z|       1|        3|        4|       5|
            |   9876L|       6|        5|        4|       5|
        When the method is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate PAYE

    @JVM
    Examples:
    | language |
    | Scala    |

    Scenario Outline: Sad Path - PAYE Refs input has invalid value
            Given the BI data input:
               |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
               |      INDUSTRIES LTD|         1152L|               [123123123000]|1100000002|100002826247|
               |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
               |BLACKWELLGROUP LT...|[1154L, 1155L]|               [111222333001]|1100000003|100000827984|
               |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
               |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
               |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
               |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
               |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
            And a PAYE refs input with invalid field:
                | payeref|mar_jobs|june_jobs|sept_jobs| INVALID|
                |   1151L|       1|  Invalid|        3|       4|
                |   1152L|       5|        6|     null|       8|
                |   1153L|       9|        1|        2|       3|
                |   1154L|       4|     null|        6|       7|
                |   1155L|       8|        9|        1|       2|
                |   1166L|       1|        1|        2|       3|
                |   3333L|       1|        1|        2|       3|
                |   1188L|       2|        2|        2|       2|
                |   1199L|    null|     null|     null|    null|
                |   5555L|    null|     null|     null|    null|
                |   1999Z|       1|        3|        4|       5|
                |   9876L|       6|        5|        4|       5|
            When the method is attempted
            Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate PAYE

        @JVM
        Examples:
        | language |
        | Scala    |
