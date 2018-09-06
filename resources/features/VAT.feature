Feature: VAT Calculation
    works out vat

    Scenario Outline: Happy Path
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

        And the VAT refs input:
           |    vatref    | turnover | record_type |
           | 555666777003 |   260    |     3       |
           | 919100010000 |    85    |     1       |
           | 999888777000 |   260    |     0       |
           | 555666777002 |   340    |     3       |
           | 555666777000 |  1000    |     1       |
           | 555666777001 |   320    |     3       |
           | 111222333001 |   590    |     3       |
           | 111222333000 |   585    |     1       |
           | 123123123000 |   390    |     0       |

        And the PAYE refs input:
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

        When VAT is calculated
        Then a VAT results table is produced:
           |       ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover|
           |1100000004|          4|        8|         null|         444|         260|        1000|         704|
           |2000000011|          2|        4|         null|        null|         390|        null|         390|
           |9900000009|          3|        5|           85|        null|        null|        null|          85|
           |1100000003|         19|       20|          585|        null|        null|        null|         585|
           |2200000002|          5|        5|         null|         555|        null|        1000|         555|

    @JVM
    Examples:
    | language |
    | Scala    |

    Scenario Outline: Sad Path - VAT refs input has invalid field
        Given the BI data input:
           |        BusinessName|      PayeRefs|                      VatRefs|       ern|          id|
           |      INDUSTRIES LTD|       [1151L]|               [123123123000]|2000000011|100002826247|
           |BLACKWELLGROUP LT...|[1155L, 1154L]|               [111222333001]|1100000003|100000827984|
           |             IBM LTD|[1166L, 1177L]|[555666777000, 5556667770001]|1100000004|100000459235|
           |         IBM LTD - 2|[1188L, 1199L]|               [555666777002]|1100000004|100000508723|
           |         IBM LTD - 3|[5555L, 3333L]|               [999888777000]|1100000004|100000508724|
           |             MBI LTD|       [9876L]|               [555666777003]|2200000002|100000601835|
           |   NEW ENTERPRISE LU|       [1999Z]|               [919100010000]|9900000009|999000508999|

        And a VAT refs input with a field that does not exist:
           |    vatref    | turnover |  INVALID    |
           | 555666777003 |   260    |     3       |
           | 919100010000 |    85    |     1       |
           | 999888777000 |   260    |     0       |
           | 555666777002 |   340    |     3       |
           | 555666777000 |  1000    |     1       |
           | 555666777001 |   320    |     3       |
           | 111222333001 |   590    |     3       |
           | 111222333000 |   585    |     1       |
           | 123123123000 |   390    |     0       |

        And the PAYE refs input:
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

        When the VAT method is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate VAT

    @JVM
    Examples:
    | language |
    | Scala    |
