package uk.gov.ons.registers.methods

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

trait TestData {
 
  val dataColNames = List("ern","entref","name","tradingstyle" ,"address1","address2","address3","address4","address5" ,"postcode" ,"legalstatus" ,"sic07" ,"paye_empees" ,"paye_jobs" ,"ent_turnover" ,"std_turnover" ,"grp_turnover" ,"cntd_turnover" ,"app_turnover" ,"prn","cell_no")
  
  val  data = List(
                  ("1100000001", "9906000015", "&EAGBBROWN"                   ,  ""  , "1 HAWRIDGE HILL COTTAGES", "THE VALE"      , "HAWRIDGE"   , "CHESHAM BUCKINGHAMSHIRE",  ""  , "HP5 3NU" , "1", "45112", "1" , "1" , "73" , "73 ", "0", "0", "0", "0.109636832", "5813"),
                  ("1100000007", "9906000275", "IBANOCTRACTS UK LTD"          ,  ""  , "1 GLYNDE PLACE"          , "HORSHAM"       , "WEST SUSSEX",       ""                 ,  ""  , "RH12 1NZ", "1", "46120", "2" , "2" , "287", "287", "0", "0", "0", "0.155647458", "5813"),
                  ("1100000099", "9906000299", "BLAH BLAH BLAH BLAH PLC"     ,   ""  , "1 High St"               , "CITY"          , "BLX"        ,       ""                 ,  ""  , "BLAH BLAH", "1", "46120", "2" , "2" , "287", "287", "0", "0", "0","0.165647458", "5813"),
                  ("1100000008", "9906000325", "TLUBARE"                      ,  ""  , "1 GORSE ROAD"            , "REYDON"        , "SOUTHWOLD"  ,       ""                 ,  ""  , "IP18 6NQ", "1", "46130", "3" , "3" , "197", "197", "0", "0", "0", "0.446872271", "5813"),
                  ("1100000004", "9906000145", "AUBASOT(CHRISTCHURCH) LIMITED",  ""  , "1 GARTH EDGE"            , "SHAWFORTH"     , "WHITWORTH"  , "ROCHDALE LANCASHIRE"   ,  ""   , "OL12 8EH", "2", "45320", "0" , "0" , "7" , "7"  , "0", "0", "0",  "0.509298879" ,"5813"),
                  ("1100000002", "9906000045", "BUEADLIING SOLUTIONS LTD"     ,  ""  , "1 HAZELWOOD LANE"        , "ABBOTS LANGLEY",    ""        ,   ""                     ,  ""  , "WD5 0HA" , "3", "45190", "1" , "0" , "100", "100", "0", "0", "0", "0.63848639" , "5813"),

                  ("1100000003", "9906000075", "JO2WMILITED"                  ,  ""  , "1 BARRASCROFTS"          , "CANONBIE"      ,    ""        ,   ""                     ,  ""  , "DG14 0RZ", "1", "45200", "1" , "0" , "56" , "56" , "0", "0", "0", "0.095639204", "5814"),
                  ("1100000005", "9906000175", "HIBAER"                       ,  ""  , "1 GEORGE SQUARE"         , "GLASGOW"       ,      ""      ,       ""                 ,  ""  , "G2 5LL"  , "1", "45400", "1" , "1" , "106", "106", "0", "0", "0", "0.147768898", "5814"),
                  ("1100000006", "9906000205", "HIBAER"                       ,  ""  , "1 GLEN ROAD"             , "HINDHEAD"      , "SURREY"     ,       ""                 ,  ""  , "GU26 6QE", "1", "46110", "1" , "1" , "297", "297", "0", "0", "0", "0.588701588", "5814"),
                  ("1100000009", "9906000355", "BUCARR"                       ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "46140", "1" , "1" , "18" , "18" , "0", "0", "0", "0.847311602", "5811"),
                  ("1100000010", "9906000405", "DCAJ&WALTON"                  ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "46150", "2" , "2" , "72" , "72" , "0", "0", "0", "0.548604086", "5814"),
                  ("1100000011", "9906000415", "&BAMCFLINT"                   ,  ""  , "1 GARENDON WAY"          , "GROBY"         , "LEICESTER"  ,       ""                 ,  ""  , "LE6 0YR" , "1", "46160", "1" , "0" , "400", "400", "0", "0", "0", "0.269071541", "5814")
    )

  val propsColNames = List("inqcode" ,"cell_no" ,"cell_desc" ,"seltype" ,"lower_class" ,"upper_class" ,"lower_size" ,"upper_size","prn_start","no_reqd")
  val props = List(
                    ("687","5814","Census","C","45111","45190","100","999999999","0.0000","0"),
                    ("687","5813","Census","P","45111","45190","100","999999999","0.5","4"),
                    ("687","5811","Admin","U","45111","45190","0","9","0.000000000","0"),
                    ("687","5812","Admin","U","45111","45190","10","49","0.000000000","0")
                    )




  def getDataDF(implicit spark:SparkSession) = {
    val dataDF = spark.createDataFrame(data).toDF(dataColNames: _*)
    dataDF
  }

  def getPropsDF(implicit spark:SparkSession) = {
    val propsDF = spark.createDataFrame(props).toDF(propsColNames: _*)
    propsDF
  }

  def expectedOutputDF(implicit spark:SparkSession) = {
    val expectedDF = spark.createDataFrame(expectedOutputData).toDF(dataColNames: _*)
    expectedDF
  }

val expectedOutputData = List(
  ("1100000002", "9906000045", "BUEADLIING SOLUTIONS LTD"     ,  ""  , "1 HAZELWOOD LANE"        , "ABBOTS LANGLEY",    ""        ,   ""                     ,  ""  , "WD5 0HA" , "3", "45190", "1" , "0" , "100", "100", "0", "0", "0", "0.63848639" , "5813"),
  ("1100000004", "9906000145", "AUBASOT(CHRISTCHURCH) LIMITED",  ""  , "1 GARTH EDGE"            , "SHAWFORTH"     , "WHITWORTH"  , "ROCHDALE LANCASHIRE"   ,  ""   , "OL12 8EH", "2", "45320", "0" , "0" , "7" , "7"  , "0", "0", "0",  "0.509298879" ,"5813"),
  ("1100000007", "9906000275", "IBANOCTRACTS UK LTD"          ,  ""  , "1 GLYNDE PLACE"          , "HORSHAM"       , "WEST SUSSEX",       ""                 ,  ""  , "RH12 1NZ", "1", "46120", "2" , "2" , "287", "287", "0", "0", "0", "0.155647458", "5813"),
  ("1100000001", "9906000015", "&EAGBBROWN"                   ,  ""  , "1 HAWRIDGE HILL COTTAGES", "THE VALE"      , "HAWRIDGE"   , "CHESHAM BUCKINGHAMSHIRE",  ""  , "HP5 3NU" , "1", "45112", "1" , "1" , "73" , "73 ", "0", "0", "0", "0.109636832", "5813"),

  ("1100000006", "9906000205", "HIBAER"                       ,  ""  , "1 GLEN ROAD"             , "HINDHEAD"      , "SURREY"     ,       ""                 ,  ""  , "GU26 6QE", "1", "46110", "1" , "1" , "297", "297", "0", "0", "0", "0.588701588", "5814"),
  ("1100000010", "9906000405", "DCAJ&WALTON"                  ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "46150", "2" , "2" , "72" , "72" , "0", "0", "0", "0.548604086", "5814"),
  ("1100000011", "9906000415", "&BAMCFLINT"                   ,  ""  , "1 GARENDON WAY"          , "GROBY"         , "LEICESTER"  ,       ""                 ,  ""  , "LE6 0YR" , "1", "46160", "1" , "0" , "400", "400", "0", "0", "0", "0.269071541", "5814"),
  ("1100000005", "9906000175", "HIBAER"                       ,  ""  , "1 GEORGE SQUARE"         , "GLASGOW"       ,      ""      ,       ""                 ,  ""  , "G2 5LL"  , "1", "45400", "1" , "1" , "106", "106", "0", "0", "0", "0.147768898", "5814"),
  ("1100000003", "9906000075", "JO2WMILITED"                  ,  ""  , "1 BARRASCROFTS"          , "CANONBIE"      ,    ""        ,   ""                     ,  ""  , "DG14 0RZ", "1", "45200", "1" , "0" , "56" , "56" , "0", "0", "0", "0.095639204", "5814")
  ).sortBy(_._20)
}
