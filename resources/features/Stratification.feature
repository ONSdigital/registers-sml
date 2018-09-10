@Stratification
Feature: Create a Stratified Frame
    This feature takes an input Frame that reflects some statistical unit type data assigned with a prn and stratifies it.
    Stratas are defined in a provided properties file. For every given strata (without regard for the selection type) a
    set of numerical range filters (e.g. payeEmployee, sic07) are imposed on the Frame. A produced stratified cell is
    then allocated a cell number (as per strata) which is appended to each of its rows.

    @SadPath
    Scenario Outline: An exception is throw when an Frame file with an invalid field type is given as the input argument
        """
        Where a required field paye_empees has a value 'invalid' causes an error. A meaningful and valid value must be a whole number.
        Required fields have required a type - violation causes failure.
        """
        Given a Frame with an invalid required field:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | invalid     | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
        And a Strata from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5816    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.539298879 | 3       |
        When a Stratified Frame creation is attempted
        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Stratify

        @JVM
        Examples:
        | language |
        | Scala    |

    @SadPath
    Scenario Outline: An exception is throw when an Stratification Properties file with an invalid field type is given as the input argument
        """
        Where a required field lower_class has a value 'invalid' causes an error. A meaningful and valid value must be a whole number.
        Required fields have required a type - violation causes failure.
        """
        Given a Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
        And a Stratification Properties file with an invalid field type:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5816    | Sample    | P       | invalid     | 45190       | 50         | 9          | 0.539298879 | 3       |
        When a Stratified Frame creation is attempted
        Then an exception in <language> is thrown for Stratified Properties due to a mismatch field type upon trying to Stratify

        @JVM
        Examples:
        | language |
        | Scala    |

    @HappyPath
    Scenario Outline: Apply sic07 and payeEmployee range filters on a Frame
        Given a Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
        And a Strata from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
        When a <language> Stratified Frame is created from a Frame
        Then a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5812    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5812    |

        @JVM
        Examples:
        | language |
        | Scala    |

    @HappyPath
    Scenario Outline: Apply sic07 and payeEmployee range filters on a Frame with the remaining unallocated units returned with -1 allocated to them
        Given a Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
        And a Strata from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5819    | Admin     | C       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
        When a <language> Stratified Frame is created from a Frame
        Then a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata and unallocated units are labelled as -1:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5819    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5819    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | -1      |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | -1      |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | -1      |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | -1      |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | -1      |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | -1      |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | -1      |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | -1      |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | -1      |

        @JVM
        Examples:
        | language |
        | Scala    |
