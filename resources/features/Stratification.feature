#@Stratification
#Feature: Create a Stratified Frame
#    This feature takes an input Frame that reflects some statistical unit type data assigned with a prn and stratifies it.
#    Stratas are defined in a provided properties file. For every given strata (without regard for the selection type) a
#    set of numerical range filters (e.g. payeEmployee, sic07) are imposed on the Frame. A produced stratified cell is
#    then allocated a cell number (as per strata) which is appended to each of its rows.
#
#    @HappyPath
#    Scenario Outline: Enterprise Unit - Apply sic07 and payeEmployee range filters on a Frame that contains null values in the paye_empees
#        """
#        null values in the payeEmployee field connot be evaluate and thus must be omitted.
#        Omitted units in this instance are denoted with -2 in their cell number field.
#        """
#        Given a Frame where some units have PayeEmployee field as null:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45220 | 40          | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45200 | 11          | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 45112 |             | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 45120 | 29          | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45138 |             | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 45240 | 41          | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 45155 |             | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 45167 | 33          | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5819    | Admin     | C       | 45111       | 45290       | 10         | 49         | 0.000000000 | 0       |
#        And a specification of unit and params:
#            |    Unit   |   Bounds    |
#            | Enterprise| paye_empees |
#        When a <language> Stratified Frame is created from a Frame
#        Then a Stratified Frame is returned with units assigned a Strata number, where the unit has a null PayeEmployee value the unit is allocated a Strata number of -2:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45138 | null        | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | -2      |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 45155 | null        | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | -2      |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 45112 | null        | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | -2      |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5819    |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45200 | 11          | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | 5819    |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 45120 | 29          | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | 5819    |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 45167 | 33          | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | 5819    |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45220 | 40          | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | 5819    |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 45240 | 41          | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | 5819    |
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @HappyPath
#    Scenario Outline: Enterprise Unit - Apply sic07 and payeEmployee range filters on a Frame with the remaining unallocated units returned with -1 allocated to them
#        Given a Frame:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
#            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
#            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5819    | Admin     | C       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
#        And a specification of unit and params:
#            | Unit       | Bounds      |
#            | Enterprise | paye_empees |
#        When a <language> Stratified Frame is created from a Frame
#        Then a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata and unallocated units are labelled as -1:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
#            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 | -1      |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 | -1      |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 | -1      |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 | -1      |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 | -1      |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 | -1      |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 | -1      |
#            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  | -1      |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 | -1      |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5819    |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5819    |
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @HappyPath
#    Scenario Outline: Reporting Unit - Apply sic07 and employees range filters on a Frame encompasing all scenarios
#        Given a Frame:
#            | rurn        | ruref       | ern        | entref     | sic07   | employees | employment | turnover | prn         | legalstatus | region | name                              | tradestyle    | address1              | address2      | address3            | address4  | address5 | postcode |
#            | 33000000051 | 49906016135 | 1100000051 | 9906016135 | 55100   | 5         | 5          | 369      | 0.158231512 | 1           | JG     | DE536LEVOPMENTS LTD               |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY |
#            | 33000000052 | 49906044845 | 1100000052 | 9906044845 | 47710   | 4         | 4          | 2150     | 0.126696302 | 1           | HH     | L9337DT                           |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY |
#            | 33000000053 | 49906050715 | 1100000053 | 9906050715 | 82990   | 0         | 0          | 392      | 0.424149751 | 1           | HH     | OOT3DOBLX LTD                     |               | 1 BOXLEY GARDENS      | BOXLEY ROAD   | PENENDEN HEATH      | MAIDSTONE |          | ME14 2BD |
#            | 33000000054 | 49906152275 | 1100000054 | 9906152275 | 71121   | 9         | 9          | 314      | 0.298446864 | 1           | GG     | OOT3DOBLX LTD                     | OOT3DOBLX.COM | 1 BRAESIDE            | NAPHILL       | HIGH WYCOMBE        | BUCKS     |          | HP14 4RY |
#            | 33000000055 | 49906210955 | 1100000055 | 9906210955 | 41201   | 1         | 1          | 8279     | 0.823342781 | 1           | GG     | OGC3VMIESRL LTD                   |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            |           |          | SS16 6TB |
#            | 33000000056 | 49906215965 | 1100000056 | 9906215965 | 71122   | 8         | 8          | 30270    | 0.968260607 | 1           | JG     | UNOC4EITS SERVICES LIMITED        |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            | ESSEX     |          | SS16 6TB |
#            | 33000000059 | 49906262705 | 1100000059 | 9906262705 | 96090   | 8         | 8          | 4566     | 0.402750262 | 1           | HH     | UNOC4EITS SERVICES LTD            |               | 1 BROOK COURT         | BLAKENEY ROAD | BECKENHAM           | KENT      |          | BR3 1HG  |
#            | 33000000063 | 49902232725 | 1100000063 | 9902232725 | 10612   | 14100     | null       | 27367068 | 0.556599574 | 1           | XX     | BIG BOX CEREAL LTD INCL VAT GROUP |               | (ROAD HAULAGE CONTRS) | BOW BRIDGE    | WATERINGBURY        | MAIDSTONE | KENT     | ME18 5ED |
#           And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5819    | Admin     | C       | 0           | 1000000     | 6          | 49         | 0.000000000 | 0       |
#        And a specification of unit and params:
#            | Unit      | Bounds     |
#            | Reporting | employment |
#        When a <language> Stratified Frame is created from a Frame
#        Then a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata and unallocated units are labelled as -1:
#            | rurn        | ruref       | ern        | entref     | sic07 | employees | employment | turnover | prn         | legalstatus | region | name                              | tradestyle    | address1              | address2      | address3            | address4  | address5 | postcode| cell_no |
#            | 33000000063 | 49902232725 | 1100000063 | 9902232725 | 10612 | 14100     | null       | 27367068 | 0.556599574 | 1           | XX     | BIG BOX CEREAL LTD INCL VAT GROUP |               | (ROAD HAULAGE CONTRS) | BOW BRIDGE    | WATERINGBURY        | MAIDSTONE | KENT     | ME18 5ED| -2      |
#            | 33000000052 | 49906044845 | 1100000052 | 9906044845 | 47710 | 4         | 4          | 2150     | 0.126696302 | 1           | HH     | L9337DT                           |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY| -1      |
#            | 33000000051 | 49906016135 | 1100000051 | 9906016135 | 55100 | 5         | 5          | 369      | 0.158231512 | 1           | JG     | DE536LEVOPMENTS LTD               |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY| -1      |
#            | 33000000053 | 49906050715 | 1100000053 | 9906050715 | 82990 | 0         | 0          | 392      | 0.424149751 | 1           | HH     | OOT3DOBLX LTD                     |               | 1 BOXLEY GARDENS      | BOXLEY ROAD   | PENENDEN HEATH      | MAIDSTONE |          | ME14 2BD| -1      |
#            | 33000000055 | 49906210955 | 1100000055 | 9906210955 | 41201 | 1         | 1          | 8279     | 0.823342781 | 1           | GG     | OGC3VMIESRL LTD                   |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            |           |          | SS16 6TB| -1      |
#            | 33000000054 | 49906152275 | 1100000054 | 9906152275 | 71121 | 9         | 9          | 314      | 0.298446864 | 1           | GG     | OOT3DOBLX LTD                     | OOT3DOBLX.COM | 1 BRAESIDE            | NAPHILL       | HIGH WYCOMBE        | BUCKS     |          | HP14 4RY| 5819    |
#            | 33000000059 | 49906262705 | 1100000059 | 9906262705 | 96090 | 8         | 8          | 4566     | 0.402750262 | 1           | HH     | UNOC4EITS SERVICES LTD            |               | 1 BROOK COURT         | BLAKENEY ROAD | BECKENHAM           | KENT      |          | BR3 1HG | 5819    |
#            | 33000000056 | 49906215965 | 1100000056 | 9906215965 | 71122 | 8         | 8          | 30270    | 0.968260607 | 1           | JG     | UNOC4EITS SERVICES LIMITED        |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            | ESSEX     |          | SS16 6TB| 5819    |
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @HappyPath
#    Scenario Outline: Enterprise List - Apply sic07 and payeEmployee range filters on a Frame
#        Given a Frame:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5812    | Admin     | U       | 45111       | 45190       | 10         | 49         | 0.000000000 | 0       |
#        And a specification of unit and params:
#            |   Unit   |  Bounds   |
#            |Enterprise|paye_empees|
#        When a <language> Stratified Frame is created from a Frame
#        Then a Stratified Frame is returned with the strata assigned the Strata number from the Stratification Strata:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         | cell_no |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 39          | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 | 5812    |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 45130 | 13          | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 | 5812    |
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @SadPath
#    Scenario Outline: Enterprise List - An exception is throw when an Frame file with an invalid field type is given as the input argument
#        """
#        Where a required field sic07 has a value 'invalid' causes an error. A meaningful and valid value must be a whole number.
#        Required fields have required a type - violation causes failure.
#        """
#        Given a Frame with an invalid required field:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07   | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | invalid | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
#            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190   | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
#            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189   | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320   | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400   | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110   | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120   | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130   | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140   | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150   | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160   | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5816    | Sample    | P       | 45111       | 45190       | 50         | 9          | 0.539298879 | 3       |
#        And a specification of unit and params:
#            |   Unit   |  Bounds   |
#            |Enterprise|paye_empees|
#        When a Stratified Frame creation is attempted
#        Then an exception in <language> is thrown for Frame due to a mismatch field type upon trying to Stratify
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @SadPath
#    Scenario Outline: Enterprise Unit - An exception is throw when an Stratification Properties file with an invalid field type is given as the input argument
#        """
#        Where a required field lower_class has a value 'invalid' causes an error. A meaningful and valid value must be a whole number.
#        Required fields have required a type - violation causes failure.
#        """
#        Given a Frame:
#            | ern        | entref     | name                          | tradingstyle | address1                 | address2       | address3    | address4                | address5 | postcode | legalstatus | sic07 | paye_empees | paye_jobs | ent_turnover | std_turnover | grp_turnover | cntd_turnover | app_turnover | prn         |
#            | 1100000001 | 9906000015 | &EAGBBROWN                    |              | 1 HAWRIDGE HILL COTTAGES | THE VALE       | HAWRIDGE    | CHESHAM BUCKINGHAMSHIRE |          | HP5 3NU  | 1           | 45112 | 1           | 1         | 73           | 73           | 0            | 0             | 0            | 0.109636832 |
#            | 1100000002 | 9906000045 | BUEADLIING SOLUTIONS LTD      |              | 1 HAZELWOOD LANE         | ABBOTS LANGLEY |             |                         |          | WD5 0HA  | 3           | 45190 | 1567        | 0         | 100          | 100          | 0            | 0             | 0            | 0.63848639  |
#            | 1100000003 | 9906000075 | JO2WMILITED                   |              | 1 BARRASCROFTS           | CANONBIE       |             |                         |          | DG14 0RZ | 1           | 45189 | 1000        | 0         | 56           | 56           | 0            | 0             | 0            | 0.095639204 |
#            | 1100000004 | 9906000145 | AUBASOT(CHRISTCHURCH) LIMITED |              | 1 GARTH EDGE             | SHAWFORTH      | WHITWORTH   | ROCHDALE LANCASHIRE     |          | OL12 8EH | 2           | 45320 | 0           | 0         | 7            | 7            | 0            | 0             | 0            | 0.509298879 |
#            | 1100000005 | 9906000175 | HIBAER                        |              | 1 GEORGE SQUARE          | GLASGOW        |             |                         |          | G2 5LL   | 1           | 45400 | 1           | 1         | 106          | 106          | 0            | 0             | 0            | 0.147768898 |
#            | 1100000006 | 9906000205 | HIBAER                        |              | 1 GLEN ROAD              | HINDHEAD       | SURREY      |                         |          | GU26 6QE | 1           | 46110 | 1           | 1         | 297          | 297          | 0            | 0             | 0            | 0.588701588 |
#            | 1100000007 | 9906000275 | IBANOCTRACTS UK LTD           |              | 1 GLYNDE PLACE           | HORSHAM        | WEST SUSSEX |                         |          | RH12 1NZ | 1           | 46120 | 2           | 2         | 287          | 287          | 0            | 0             | 0            | 0.155647458 |
#            | 1100000008 | 9906000325 | TLUBARE                       |              | 1 GORSE ROAD             | REYDON         | SOUTHWOLD   |                         |          | IP18 6NQ | 1           | 46130 | 3           | 3         | 197          | 197          | 0            | 0             | 0            | 0.446872271 |
#            | 1100000009 | 9906000355 | BUCARR                        |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46140 | 1           | 1         | 18           | 18           | 0            | 0             | 0            | 0.847311602 |
#            | 1100000010 | 9906000405 | DCAJ&WALTON                   |              | 1 GRANVILLE AVENUE       | LONG EATON     | NOTTINGHAM  |                         |          | NG10 4HA | 1           | 46150 | 2           | 2         | 72           | 72           | 0            | 0             | 0            | 0.548604086 |
#            | 1100000011 | 9906000415 | &BAMCFLINT                    |              | 1 GARENDON WAY           | GROBY          | LEICESTER   |                         |          | LE6 0YR  | 1           | 46160 | 1           | 0         | 400          | 400          | 0            | 0             | 0            | 0.269071541 |
#        And a Stratification Properties file with an invalid field type:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5816    | Sample    | P       | invalid     | 45190       | 50         | 9          | 0.539298879 | 3       |
#        And a specification of unit and params:
#            |   Unit   |  Bounds   |
#            |Enterprise|paye_empees|
#        When a Stratified Frame creation is attempted
#        Then an exception in <language> is thrown for Stratified Properties due to a mismatch field type upon trying to Stratify
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
#
#    @SadPath
#    Scenario Outline: Enterprise Unit - An exception is throw when a specification of unit and params file with an invalid field type is given as the input argument
#        """
#        Where a required field lower_class has a value 'invalid' causes an error. A meaningful and valid value must be a whole number.
#        Required fields have required a type - violation causes failure.
#        """
#        Given a Frame:
#            | rurn        | ruref       | ern        | entref     | sic07   | employees | employment | turnover | prn         | legalstatus | region | name                              | tradestyle    | address1              | address2      | address3            | address4  | address5 | postcode |
#            | 33000000051 | 49906016135 | 1100000051 | 9906016135 | 55100   | 5         | 5          | 369      | 0.158231512 | 1           | JG     | DE536LEVOPMENTS LTD               |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY |
#            | 33000000052 | 49906044845 | 1100000052 | 9906044845 | 47710   | 4         | 4          | 2150     | 0.126696302 | 1           | HH     | L9337DT                           |               | 1 BORLASES COTTAGES   | MILLEY ROAD   | WALTHAM ST LAWRENCE | READING   |          | RG10 0JY |
#            | 33000000053 | 49906050715 | 1100000053 | 9906050715 | 82990   | 0         | 0          | 392      | 0.424149751 | 1           | HH     | OOT3DOBLX LTD                     |               | 1 BOXLEY GARDENS      | BOXLEY ROAD   | PENENDEN HEATH      | MAIDSTONE |          | ME14 2BD |
#            | 33000000054 | 49906152275 | 1100000054 | 9906152275 | 71121   | 9         | 9          | 314      | 0.298446864 | 1           | GG     | OOT3DOBLX LTD                     | OOT3DOBLX.COM | 1 BRAESIDE            | NAPHILL       | HIGH WYCOMBE        | BUCKS     |          | HP14 4RY |
#            | 33000000055 | 49906210955 | 1100000055 | 9906210955 | 41201   | 1         | 1          | 8279     | 0.823342781 | 1           | GG     | OGC3VMIESRL LTD                   |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            |           |          | SS16 6TB |
#            | 33000000056 | 49906215965 | 1100000056 | 9906215965 | 71122   | 8         | 8          | 30270    | 0.968260607 | 1           | JG     | UNOC4EITS SERVICES LIMITED        |               | 1 BRITTEN CLOSE       | LANGDON HILLS | BASILDON            | ESSEX     |          | SS16 6TB |
#            | 33000000059 | 49906262705 | 1100000059 | 9906262705 | 96090   | 8         | 8          | 4566     | 0.402750262 | 1           | HH     | UNOC4EITS SERVICES LTD            |               | 1 BROOK COURT         | BLAKENEY ROAD | BECKENHAM           | KENT      |          | BR3 1HG  |
#            | 33000000063 | 49902232725 | 1100000063 | 9902232725 | 10612   | 14100     | null       | 27367068 | 0.556599574 | 1           | XX     | BIG BOX CEREAL LTD INCL VAT GROUP |               | (ROAD HAULAGE CONTRS) | BOW BRIDGE    | WATERINGBURY        | MAIDSTONE | KENT     | ME18 5ED |
#        And a Strata from Stratification Properties file:
#            | inqcode | cell_no | cell_desc | seltype | lower_class | upper_class | lower_size | upper_size | prn_start   | no_reqd |
#            | 687     | 5819    | Admin     | C       | 0           | 1000000     | 6          | 49         | 0.000000000 | 0       |
#        And a specification of unit and params:
#            | Unit      | Bounds     |
#            |Enterprise|    invalid|
#        When a Stratified Frame creation is attempted
#        Then an exception in <language> is thrown for Stratified Properties due to a mismatch field type upon trying to Stratify
#
#        @JVM
#        Examples:
#        | language |
#        | Scala    |
