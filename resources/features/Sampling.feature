@Sampling
Feature: Creating a Sample
    The Sample creation method will ingest a Stratified Frame and a set of Stratification Parameters to produce an output
    Sample. Each of the Stratification Stratas will have a selection type assigned (Universe, Census, PRN). Only those
    strata marked as ‘Census’ or ‘PRN Sampling’ are then processed. In the case of PRN Sampling, the strata is sorted by
    ‘prn’ as a decimal type in ascending order; the prn start point from the selection parameters is identified and units
    are ‘selected’ in prn order until the number required is achieved. Census strata require all units in the strata to be
    returned. The selections for each strata are output collectively in a single DataFrame with the strata ID (cell
    number) appended to each record based on which strata it belongs to.

    @HappyPath
    Scenario Outline: A strata for Census is created
        Given a Stratified Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5813    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5813    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5813    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5814    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5814    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5813    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5813    |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5811    |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5814    |
        And a Strata of selection type Census from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5814    | Census    | C       | 45111       | 45190       | 100        | 999999999  | 0.000000000 | 0       |
            | 687     | 5811    | Admin     | U       | 45111       | 45190       | 0          | 9          | 0.000000000 | 0       |
            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample containing the Sample selection from the Census strata is returned:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5814    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5814    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5814    |

        @JVM
        Examples:
        | language |
        | Scala    |

    @HappyPath
    Scenario Outline: A strata for Prn-Sampling is created
        Given a Stratified Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5813    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5813    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5813    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5814    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5814    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5813    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5813    |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5811    |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5814    |
        And a Strata of selection type Prn-Sampling from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5813    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.129110904 | 4       |
            | 687     | 5811    | Admin     | U       | 45111       | 45190       | 0          | 9          | 0.000000000 | 0       |
            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample containing the Sample Size from the Prn-Sampling strata is returned:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5813    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5813    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5813    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5813    |

        @JVM
        Examples:
        | language |
        | Scala    |

    @HappyPath
    Scenario Outline: Strata contains a Sample Size parameter that exceeds the end of the sorted PRN line.
        Given a Stratified Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5816    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5816    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5816    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5814    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5814    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5816    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5816    |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5811    |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5814    |
        And a Strata of selection type Prn-Sampling from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5816    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.539298879 | 3       |
            | 687     | 5811    | Admin     | U       | 45111       | 45190       | 0          | 9          | 0.000000000 | 0       |
            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample containing the Sample Size from the Prn-Sampling strata is returned:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5816    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5816    |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5816    |

        @JVM
        Examples:
        | language |
        | Scala    |

    @HappyPath
    Scenario Outline: A Strata with a Sample Size that exceeds the Stratified Frame population is replace with sample size #with an error stating target Sample Size is greater than strata size is logged
        Given a Stratified Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5816    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5819    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5819    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5816    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5819    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5819    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5816    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5819    |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5819    |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5819    |
        And a Strata of selection type Prn-Sampling from Stratification Properties file:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | 5819    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.129177704 | 100000  |
        When a <language> Sample is created from a Stratified Frame
        Then a Sample containing the total population in the strata is returned:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5819    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5819    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5819    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5819    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5819    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5819    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5819    |

        @JVM
        Examples:
        | language |
        | Scala    |

#    @SadPath
#    Scenario Outline: An exception is throw when a Stratified Frame file with an invalid field type is given as the input argument
#        """
#        Where a required field cell_no has a value 'invalid' causes an error. A meaningful and valid value must be a natural number.
#        Required fields have required a type - violation causes failure.
#        """
#        Given a Stratified Frame with an invalid required field:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
#            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5813    |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5813    |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | invalid |
#            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5816    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.539298879 | 3       |
#            | 687     | 5811    | Admin     | U       | 45111       | 45190       | 0          | 9          | 0.000000000 | 0       |
#            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
#        When a Sample creation is attempted
#        Then an exception in <language> is thrown for Stratified Frame due to a mismatch field type upon trying to Sample
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |

    @SadPath
    Scenario Outline: An exception is throw when an Stratification Properties file with an invalid field type is given as the input argument
        """
        Where a required field prn_start has a value 'invalid' causes an error. A meaningful and valid value must be a big decimal.
        Required fields have required a type - violation causes failure.
        """
        Given a Stratified Frame:
            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5816    |
            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1           | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | 5816    |
            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45200 | 1           | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | 5814    |
            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5816    |
            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5814    |
            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | 5814    |
            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5816    |
            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5816    |
            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5811    |
            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | 5814    |
            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5814    |
        And a Stratification Properties file with an invalid field type:
            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
            | 687     | invalid | Sample    | P       | 45111       | 45190       | 50         | 9          | invalid     | 3       |
        When a Sample creation is attempted
        Then an exception in <language> is thrown for Stratified Properties due to a mismatch field type upon trying to Sample

        @JVM
        Examples:
        | language |
        | Scala    |

