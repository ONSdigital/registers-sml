Feature: VAT Calculation
    The VAT Calculation method will take information from the BI data, VAT Refs and PAYE employees & jobs input tables calculating various turnovers.

 @HappyPath
    Scenario Outline: Happy Path - A combined Vat & Paye table is calculated
        Given the Legal unit input with vat:
          |BusinessName|                  PayeRefs|                   VatRefs|       ern|              id|
          |  Business 1|                846SZ24053|              848723100000|1100000001|1000100001234567|
          | Business 2A|                    846T10|388206724000, 388206724001|1100000002|1000100002345678|
          | Business 2B|                 083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
          | Business 3A| 070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
          | Business 3B|                  073S7501|              741144071002|1100000005|1000100005678901|
          | Business 3C|                  120E7131|              741144071003|1100000004|1000100006789012|
          | Business 6A|                 120O40781|              142672964000|1100000006|1000100007890123|
          | Business 6B|                 581MC4245|              142672964001|1100000006|1000100008901234|
          | Business 6C|                      null|              142672964002|1100000006|1000100009012345|
          | Business 6D|                      null|              524566793000|1100000006|1000100000123456|
          | Business 7A|                419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
          | Business 7B|       581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
        And the VAT refs input:
          |      vatref|turnover|record_type|
          |848723100000| 6932069|          0|
          |388206724000| 1925250|          1|
          |388206724001|   15432|          3|
          |388206724002|   80000|          3|
          |388206724003|    1200|          3|
          |741144071000|  293018|          1|
          |741144071001|   78596|          3|
          |741144071002|   52154|          3|
          |741144071003|     127|          3|
          |142672964000|26208213|          1|
          |142672964001|   36521|          3|
          |142672964002|   98562|          3|
          |524566793000|   61623|          0|
          |618020279000|  713204|          1|
          |618020279001|    3561|          3|
          |618020279002|    1265|          3|
          |644356629000|    2481|          0|
        And the PAYE input:
          |       ern|paye_empees|paye_jobs|
          |1100000001|      25469|    25484|
          |1100000002|       3721|     3751|
          |1100000003|      29261|    28699|
          |1100000005|          6|        5|
          |1100000004|     239675|   239675|
          |1100000006|      11916|    11946|
          |1100000007|        365|      365|
          |1100000008|        480|      495|
        When VAT is calculated
        Then a combination of the PAYE and VAT results tables is produced:
          |       ern|paye_empees|paye_jobs|cntd_turnover|app_turnover|std_turnover|grp_turnover|ent_turnover|
          |1100000006|      11916|    11946|     26208213|        null|       61623|        null|    26269836|
          |1100000001|      25469|    25484|         null|        null|     6932069|        null|     6932069|
          |1100000008|        480|      495|         null|      405134|        2481|      713204|      407615|
          |1100000004|     239675|   239675|         null|      261131|        null|      293018|      261131|
          |1100000003|      29261|    28699|         null|       31880|        null|      293018|       31880|
          |1100000005|          6|        5|         null|           7|        null|      293018|           7|
          |1100000002|       3721|     3751|      1925250|        null|        null|        null|     1925250|
          |1100000007|        365|      365|         null|      308070|        null|      713204|      308070|

    @JVM
    Examples:
    | language |
    | Scala    |

    @HappyPath
    Scenario Outline: Happy Path - Contained Turnover
       Given the Legal unit input with vat:
          |BusinessName|                 PayeRefs|                   VatRefs|       ern|              id|
          |  Business 1|               846SZ24053|              848723100000|1100000001|1000100001234567|
          | Business 2A|                   846T10|388206724000, 388206724001|1100000002|1000100002345678|
          | Business 2B|                083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
          | Business 3A|070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
          | Business 3B|                 073S7501|              741144071002|1100000005|1000100005678901|
          | Business 3C|                 120E7131|              741144071003|1100000004|1000100006789012|
          | Business 6A|                120O40781|              142672964000|1100000006|1000100007890123|
          | Business 6B|                581MC4245|              142672964001|1100000006|1000100008901234|
          | Business 6C|                     null|              142672964002|1100000006|1000100009012345|
          | Business 6D|                     null|              524566793000|1100000006|1000100000123456|
          | Business 7A|               419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
          | Business 7B|      581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
       And the VAT refs input:
          |      vatref|turnover|record_type|
          |848723100000| 6932069|          0|
          |388206724000| 1925250|          1|
          |388206724001|   15432|          3|
          |388206724002|   80000|          3|
          |388206724003|    1200|          3|
          |741144071000|  293018|          1|
          |741144071001|   78596|          3|
          |741144071002|   52154|          3|
          |741144071003|     127|          3|
          |142672964000|26208213|          1|
          |142672964001|   36521|          3|
          |142672964002|   98562|          3|
          |524566793000|   61623|          0|
          |618020279000|  713204|          1|
          |618020279001|    3561|          3|
          |618020279002|    1265|          3|
          |644356629000|    2481|          0|
       And the PAYE input:
         |       ern|paye_empees|paye_jobs|
         |1100000001|      25469|    25484|
         |1100000002|       3721|     3751|
         |1100000003|      29261|    28699|
         |1100000005|          6|        5|
         |1100000004|     239675|   239675|
         |1100000006|      11916|    11946|
         |1100000007|        365|      365|
         |1100000008|        480|      495|
       When Contained Turnover is calculated
       Then a Contained Turnover results table is produced:
          |       ern|cntd_turnover|
          |1100000006|     26208213|
          |1100000001|         null|
          |1100000008|         null|
          |1100000004|         null|
          |1100000003|         null|
          |1100000005|         null|
          |1100000002|      1925250|
          |1100000007|         null|

    @JVM
    Examples:
    | language |
    | Scala    |


     @HappyPath
     Scenario Outline: Happy Path - apportioned turnover
        Given the Legal unit input with vat:
           |BusinessName|                 PayeRefs|                   VatRefs|       ern|              id|
           |  Business 1|               846SZ24053|              848723100000|1100000001|1000100001234567|
           | Business 2A|                   846T10|388206724000, 388206724001|1100000002|1000100002345678|
           | Business 2B|                083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
           | Business 3A|070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
           | Business 3B|                 073S7501|              741144071002|1100000005|1000100005678901|
           | Business 3C|                 120E7131|              741144071003|1100000004|1000100006789012|
           | Business 6A|                120O40781|              142672964000|1100000006|1000100007890123|
           | Business 6B|                581MC4245|              142672964001|1100000006|1000100008901234|
           | Business 6C|                     null|              142672964002|1100000006|1000100009012345|
           | Business 6D|                     null|              524566793000|1100000006|1000100000123456|
           | Business 7A|               419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
           | Business 7B|      581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
        And the VAT refs input:
           |      vatref|turnover|record_type|
           |848723100000| 6932069|          0|
           |388206724000| 1925250|          1|
           |388206724001|   15432|          3|
           |388206724002|   80000|          3|
           |388206724003|    1200|          3|
           |741144071000|  293018|          1|
           |741144071001|   78596|          3|
           |741144071002|   52154|          3|
           |741144071003|     127|          3|
           |142672964000|26208213|          1|
           |142672964001|   36521|          3|
           |142672964002|   98562|          3|
           |524566793000|   61623|          0|
           |618020279000|  713204|          1|
           |618020279001|    3561|          3|
           |618020279002|    1265|          3|
           |644356629000|    2481|          0|
        And the PAYE input:
           |       ern|paye_empees|paye_jobs|
           |1100000001|      25469|    25484|
           |1100000002|       3721|     3751|
           |1100000003|      29261|    28699|
           |1100000005|          6|        5|
           |1100000004|     239675|   239675|
           |1100000006|      11916|    11946|
           |1100000007|        365|      365|
           |1100000008|        480|      495|
        When Apportioned Turnover is calculated
        Then an Apportioned Turnover results table is produced:
           |       ern|app_turnover|
           |1100000006|        null|
           |1100000001|        null|
           |1100000008|      405134|
           |1100000004|      261131|
           |1100000003|       31880|
           |1100000005|           7|
           |1100000002|        null|
           |1100000007|      308070|

    @JVM
    Examples:
    | language |
    | Scala    |

 @HappyPath
    Scenario Outline: Happy Path - Standard Turnover
       Given the Legal unit input with vat:
          |BusinessName|                 PayeRefs|                   VatRefs|       ern|              id|
          |  Business 1|               846SZ24053|              848723100000|1100000001|1000100001234567|
          | Business 2A|                   846T10|388206724000, 388206724001|1100000002|1000100002345678|
          | Business 2B|                083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
          | Business 3A|070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
          | Business 3B|                 073S7501|              741144071002|1100000005|1000100005678901|
          | Business 3C|                 120E7131|              741144071003|1100000004|1000100006789012|
          | Business 6A|                120O40781|              142672964000|1100000006|1000100007890123|
          | Business 6B|                581MC4245|              142672964001|1100000006|1000100008901234|
          | Business 6C|                     null|              142672964002|1100000006|1000100009012345|
          | Business 6D|                     null|              524566793000|1100000006|1000100000123456|
          | Business 7A|               419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
          | Business 7B|      581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
       And the VAT refs input:
          |      vatref|turnover|record_type|
          |848723100000| 6932069|          0|
          |388206724000| 1925250|          1|
          |388206724001|   15432|          3|
          |388206724002|   80000|          3|
          |388206724003|    1200|          3|
          |741144071000|  293018|          1|
          |741144071001|   78596|          3|
          |741144071002|   52154|          3|
          |741144071003|     127|          3|
          |142672964000|26208213|          1|
          |142672964001|   36521|          3|
          |142672964002|   98562|          3|
          |524566793000|   61623|          0|
          |618020279000|  713204|          1|
          |618020279001|    3561|          3|
          |618020279002|    1265|          3|
          |644356629000|    2481|          0|
       And the PAYE input:
          |       ern|paye_empees|paye_jobs|
          |1100000001|      25469|    25484|
          |1100000002|       3721|     3751|
          |1100000003|      29261|    28699|
          |1100000005|          6|        5|
          |1100000004|     239675|   239675|
          |1100000006|      11916|    11946|
          |1100000007|        365|      365|
          |1100000008|        480|      495|
       When Standard Turnover is calculated
       Then a Standard Turnover results table is produced:
          |       ern| std_turnover|
          |1100000006|        61623|
          |1100000001|      6932069|
          |1100000008|         2481|
          |1100000004|         null|
          |1100000003|         null|
          |1100000005|         null|
          |1100000002|         null|
          |1100000007|         null|


    @JVM
    Examples:
    | language |
    | Scala    |


    @HappyPath
    Scenario Outline: Happy Path - group turnover
       Given the Legal unit input with vat:
          |BusinessName|                 PayeRefs|                   VatRefs|       ern|              id|
          |  Business 1|               846SZ24053|              848723100000|1100000001|1000100001234567|
          | Business 2A|                   846T10|388206724000, 388206724001|1100000002|1000100002345678|
          | Business 2B|                083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
          | Business 3A|070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
          | Business 3B|                 073S7501|              741144071002|1100000005|1000100005678901|
          | Business 3C|                 120E7131|              741144071003|1100000004|1000100006789012|
          | Business 6A|                120O40781|              142672964000|1100000006|1000100007890123|
          | Business 6B|                581MC4245|              142672964001|1100000006|1000100008901234|
          | Business 6C|                     null|              142672964002|1100000006|1000100009012345|
          | Business 6D|                     null|              524566793000|1100000006|1000100000123456|
          | Business 7A|               419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
          | Business 7B|      581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
       And the VAT refs input:
          |      vatref|turnover|record_type|
          |848723100000| 6932069|          0|
          |388206724000| 1925250|          1|
          |388206724001|   15432|          3|
          |388206724002|   80000|          3|
          |388206724003|    1200|          3|
          |741144071000|  293018|          1|
          |741144071001|   78596|          3|
          |741144071002|   52154|          3|
          |741144071003|     127|          3|
          |142672964000|26208213|          1|
          |142672964001|   36521|          3|
          |142672964002|   98562|          3|
          |524566793000|   61623|          0|
          |618020279000|  713204|          1|
          |618020279001|    3561|          3|
          |618020279002|    1265|          3|
          |644356629000|    2481|          0|
       And the PAYE input:
          |       ern|paye_empees|paye_jobs|
          |1100000001|      25469|    25484|
          |1100000002|       3721|     3751|
          |1100000003|      29261|    28699|
          |1100000005|          6|        5|
          |1100000004|     239675|   239675|
          |1100000006|      11916|    11946|
          |1100000007|        365|      365|
          |1100000008|        480|      495|
       When Group Turnover is calculated
       Then a Group Turnover results table is produced:
          |       ern|grp_turnover|
          |1100000006|        null|
          |1100000001|        null|
          |1100000008|      713204|
          |1100000004|      293018|
          |1100000003|      293018|
          |1100000005|      293018|
          |1100000002|        null|
          |1100000007|      713204|

    @JVM
    Examples:
    | language |
    | Scala    |

    @SadPath
    Scenario Outline: Sad Path - VAT refs input has invalid field
        Given the Legal unit input with vat:
          |BusinessName|                 PayeRefs|                   VatRefs|       ern|              id|
          |  Business 1|               846SZ24053|              848723100000|1100000001|1000100001234567|
          | Business 2A|                   846T10|388206724000, 388206724001|1100000002|1000100002345678|
          | Business 2B|                083NU5010|388206724002, 388206724003|1100000002|1000100003456789|
          | Business 3A|070EXB497, 083S7501, 8751|741144071000, 741144071001|1100000003|1000100004567890|
          | Business 3B|                 073S7501|              741144071002|1100000005|1000100005678901|
          | Business 3C|                 120E7131|              741144071003|1100000004|1000100006789012|
          | Business 6A|                120O40781|              142672964000|1100000006|1000100007890123|
          | Business 6B|                581MC4245|              142672964001|1100000006|1000100008901234|
          | Business 6C|                     null|              142672964002|1100000006|1000100009012345|
          | Business 6D|                     null|              524566793000|1100000006|1000100000123456|
          | Business 7A|               419YZ08106|618020279000, 618020279001|1100000007|1000100009876543|
          | Business 7B|      581MS4467, 120T2262|618020279002, 644356629000|1100000008|1000100008765432|
        And a VAT refs input with a field that does not exist:
           |      vatref|turnover|  INVALID  |
           |848723100000| 6932069|          0|
           |388206724000| 1925250|          1|
           |388206724001|   15432|          3|
           |388206724002|   80000|          3|
           |388206724003|    1200|          3|
           |741144071000|  293018|          1|
           |741144071001|   78596|          3|
           |741144071002|   52154|          3|
           |741144071003|     127|          3|
           |142672964000|26208213|          1|
           |142672964001|   36521|          3|
           |142672964002|   98562|          3|
           |524566793000|   61623|          0|
           |618020279000|  713204|          1|
           |618020279001|    3561|          3|
           |618020279002|    1265|          3|
           |644356629000|    2481|          0|

        And the PAYE input:
           |       ern|paye_empees|paye_jobs|
           |1100000001|      25469|    25484|
           |1100000002|       3721|     3751|
           |1100000003|      29261|    28699|
           |1100000005|          6|        5|
           |1100000004|     239675|   239675|
           |1100000006|      11916|    11946|
           |1100000007|        365|      365|
           |1100000008|        480|      495|

        When the VAT method is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Calculate VAT

    @JVM
    Examples:
    | language |
    | Scala    |
