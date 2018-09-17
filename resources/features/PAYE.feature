Feature: PAYE Calculation
    The PAYE Calculation method will take information from the BI data and PAYE refs tables to calculate paye_employees and paye_jobs

    paye_employees takes sum of the average number of jobs per quarter across an id for each ern, so for each id sum the jobs for all the quaters and divide by the number of non-null quaters then sum for each erm

    paye_jobs takes the sum of the last quater of each payeref for each ern, so find the values of the last quater of each payeref per ern and sum them

    @HappyPath
    Scenario Outline: Happy Path - with nulls
        Given the BI data input:
           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
           |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
           |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
           |BLACKWELLGROUP LT...|[1155L, 1154L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|       [1166L]|[555666777000, 5556667770001]|1100000004|100000459235|
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
        When the PAYE method is applied
        Then a PAYE results table is produced:
           |       ern|paye_empees|paye_jobs|
           |2200000002|          5|        5|
           |1100000003|         17|       20|
           |9900000009|          3|        5|
           |2000000011|          2|        4|
           |1100000004|          5|        8|

    @JVM
    Examples:
    | language |
    | Scala    |

#   @HappyPath
#   Scenario Outline: Happy Path - without nulls
#        Given the BI data input:
#           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
#           |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
#           |BLACKWELLGROUP LT...|       [1153L]|               [111222333000]|1100000003|100000246017|
#           |BLACKWELLGROUP LT...|       [1155L]|               [111222333001]|1100000003|100000827984|
#           |             IBM LTD|       [1166L]|[555666777000, 5556667770001]|1100000004|100000459235|
#           |         IBM LTD - 2|       [1188L]|               [555666777002]|1100000004|100000508723|
#           |         IBM LTD - 3|       [3333L]|               [999888777000]|1100000004|100000508724|
#           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
#           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
#
#        And the PAYE refs input with nulls:
#           | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
#           |   1151L|       1|        2|        3|       4|
#           |   1153L|       9|        1|        2|       3|
#           |   1155L|       8|        9|        1|       2|
#           |   1166L|       1|        1|        2|       3|
#           |   3333L|       1|        1|        2|       3|
#           |   1188L|       2|        2|        2|       2|
#           |   1999Z|       1|        3|        4|       5|
#           |   9876L|       6|        5|        4|       5|
#        When the PAYE method is applied
#        Then a PAYE results table is produced:
#           |       ern|paye_empees|paye_jobs|
#           |2200000002|          5|        5|
#           |1100000003|          8|        5|
#           |9900000009|          3|        5|
#           |2000000011|          2|        4|
#           |1100000004|          5|        8|
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @HappyPath
#    Scenario Outline: Happy Path - PAYE Refs input has no missing PAYE units referenced in BI data input
#       Given the BI data input:
#           |        BusinessName|      PayeRefs|       VatRefs|       ern|          id|
#           |BLACKWELLGROUP LT...|[1152L, 1153L]|[111222333000]|1100000003|100000246017|
#           |BLACKWELLGROUP LT...|[1154L, 1155L]|[111222333001]|1100000003|100000827984|
#
#       And a PAYE refs input with missing PAYE unit:
#           | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
#           |   1152L|       5|        6|     null|       8|
#           |   1154L|       4|     null|        6|       7|
#           |   1153L|       9|        1|        2|       3|
#           |   1155L|       8|        9|        1|       2|
#       When the PAYE method is applied
#       Then a PAYE results table is produced:
#            |       ern|paye_empees|paye_jobs|
#            |1100000003|         17|       20|
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @SadPath
#    Scenario Outline: Sad Path - PAYE Refs input has missing PAYE units referenced in BI data input
#       Given the BI data input:
#             |        BusinessName|      PayeRefs|       VatRefs|       ern|          id|
#             |BLACKWELLGROUP LT...|[1152L, 1153L]|[111222333000]|1100000003|100000246017|
#             |BLACKWELLGROUP LT...|[1154L, 1155L, 1166L, 1084843, uihewhe]|[111222333001]|1100000003|100000827984|
#
#       And a PAYE refs input with missing PAYE unit:
#             | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
#             |   1152L|       5|        6|     null|       8|
#             |   1153L|       9|        1|        2|       3|
#             |   1154L|       4|     null|        6|       7|
#
#       When the PAYE method is attempted
#       Then an exception in <language> is thrown for results table due to missing PAYE unit upon trying to Calculate PAYE
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @SadPath
#    Scenario Outline: Sad Path - BI Data input has invalid field
#        Given the BI data input:
#            |        BusinessName|      PayeRefs|                      VatRefs|   INVALID|          id|
#            |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
#            |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
#            |BLACKWELLGROUP LT...|[1155L, 1154L]|               [111222333001]|1100000003|100000827984|
#            |             IBM LTD|       [1166L]|[555666777000, 5556667770001]|1100000004|100000459235|
#            |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
#            |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
#            |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
#            |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
#        And a PAYE refs input with invalid field:
#            | payeref|mar_jobs|june_jobs|sept_jobs|dec_jobs|
#            |   1151L|       1|        2|        3|       4|
#            |   1152L|       5|        6|     null|       8|
#            |   1153L|       9|        1|        2|       3|
#            |   1154L|       4|     null|        6|       7|
#            |   1155L|       8|        9|        1|       2|
#            |   1166L|       1|        1|        2|       3|
#            |   3333L|       1|        1|        2|       3|
#            |   1188L|       2|        2|        2|       2|
#            |   1199L|    null|     null|     null|    null|
#            |   5555L|    null|     null|     null|    null|
#            |   1999Z|       1|        3|        4|       5|
#            |   9876L|       6|        5|        4|       5|
#        When the PAYE method is attempted
#        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate PAYE
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
#    @SadPath
#    Scenario Outline: Sad Path - PAYE Refs input has invalid field
#        Given the BI data input:
#            |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
#            |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
#            |BLACKWELLGROUP LT...|[1152L, 1153L]|               [111222333000]|1100000003|100000246017|
#            |BLACKWELLGROUP LT...|[1155L, 1154L]|               [111222333001]|1100000003|100000827984|
#            |             IBM LTD|       [1166L]|[555666777000, 5556667770001]|1100000004|100000459235|
#            |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
#            |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
#            |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
#            |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|
#        And a PAYE refs input with invalid field:
#            | payeref|mar_jobs|june_jobs|sept_jobs|INVALID |
#            |   1151L|       1|        2|        3|       4|
#            |   1152L|       5|        6|     null|       8|
#            |   1153L|       9|        1|        2|       3|
#            |   1154L|       4|     null|        6|       7|
#            |   1155L|       8|        9|        1|       2|
#            |   1166L|       1|        1|        2|       3|
#            |   3333L|       1|        1|        2|       3|
#            |   1188L|       2|        2|        2|       2|
#            |   1199L|    null|     null|     null|    null|
#            |   5555L|    null|     null|     null|    null|
#            |   1999Z|       1|        3|        4|       5|
#            |   9876L|       6|        5|        4|       5|
#        When the PAYE method is attempted
#        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate PAYE
#
#    @JVM
#    Examples:
#    | language |
#    | Scala    |
#
