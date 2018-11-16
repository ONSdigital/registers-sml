package uk.gov.ons.registers.methods

import org.apache.spark.sql.SparkSession

trait TestStratificationData {
  val inputDataColNames = List("ern","entref","name","tradingstyle" ,"address1","address2","address3","address4","address5" ,"postcode" ,"legalstatus" ,"sic07" ,"paye_empees" ,"paye_jobs" ,"ent_turnover" ,"std_turnover" ,"grp_turnover" ,"cntd_turnover" ,"app_turnover" ,"prn")
  val ouputDataColNames = List("ern","entref","name","tradingstyle" ,"address1","address2","address3","address4","address5" ,"postcode" ,"legalstatus" ,"sic07" ,"paye_empees" ,"paye_jobs" ,"ent_turnover" ,"std_turnover" ,"grp_turnover" ,"cntd_turnover" ,"app_turnover" ,"prn","cell_no")
  val  data = List(
    ("1100000001", "9906000015", "&EAGBBROWN"                   ,  ""  , "1 HAWRIDGE HILL COTTAGES", "THE VALE"      , "HAWRIDGE"   , "CHESHAM BUCKINGHAMSHIRE",  ""  , "HP5 3NU" , "1", "45112", "39" , "1" , "73" , "73 ", "0", "0", "0", "0.109636832"),
    ("1100000004", "9906000145", "AUBASOT(CHRISTCHURCH) LIMITED",  ""  , "1 GARTH EDGE"            , "SHAWFORTH"     , "WHITWORTH"  , "ROCHDALE LANCASHIRE"   ,  ""   , "OL12 8EH", "2", "45220", "40" , "0" , "7" , "7"  , "0", "0", "0",  "0.509298879"),
    ("1100000005", "9906000175", "HIBAER"                       ,  ""  , "1 GEORGE SQUARE"         , "GLASGOW"       ,      ""      ,       ""                 ,  ""  , "G2 5LL"  , "1", "45200", "11" , "1" , "106", "106", "0", "0", "0", "0.147768898"),
    ("1100000006", "9906000205", "HIBAER"                       ,  ""  , "1 GLEN ROAD"             , "HINDHEAD"      , "SURREY"     ,       ""                 ,  ""  , "GU26 6QE", "1", "45112", "" , "1" , "297", "297", "0", "0", "0", "0.588701588"),
    ("1100000007", "9906000275", "IBANOCTRACTS UK LTD"          ,  ""  , "1 GLYNDE PLACE"          , "HORSHAM"       , "WEST SUSSEX",       ""                 ,  ""  , "RH12 1NZ", "1", "45120", "29" , "2" , "287", "287", "0", "0", "0", "0.155647458"),
    ("1100000008", "9906000325", "TLUBARE"                      ,  ""  , "1 GORSE ROAD"            , "REYDON"        , "SOUTHWOLD"  ,       ""                 ,  ""  , "IP18 6NQ", "1", "45138", "" , "3" , "197", "197", "0", "0", "0", "0.446872271"),
    ("1100000009", "9906000355", "BUCARR"                       ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "45240", "41" , "1" , "18" , "18" , "0", "0", "0", "0.847311602"),
    ("1100000010", "9906000405", "DCAJ&WALTON"                  ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "45155", "" , "2" , "72" , "72" , "0", "0", "0", "0.548604086"),
    ("1100000011", "9906000415", "&BAMCFLINT"                   ,  ""  , "1 GARENDON WAY"          , "GROBY"         , "LEICESTER"  ,       ""                 ,  ""  , "LE6 0YR" , "1", "45167", "33" , "0" , "400", "400", "0", "0", "0", "0.269071541")
  )

  val propsColNames = List("inqcode" ,"cell_no" ,"cell_desc" ,"seltype" ,"lower_class" ,"upper_class" ,"lower_size" ,"upper_size","prn_start","no_reqd")
  val props =         List(("687","5819","Admin","C","45111","45290","10","49","0.000000000","0"))

  val expectedOutputData = List(
    ("1100000001", "9906000015", "&EAGBBROWN"                   ,  ""  , "1 HAWRIDGE HILL COTTAGES", "THE VALE"      , "HAWRIDGE"   , "CHESHAM BUCKINGHAMSHIRE",  ""  , "HP5 3NU" , "1", "45112", "39" , "1" , "73" , "73 ", "0", "0", "0", "0.109636832","5819"),
    ("1100000004", "9906000145", "AUBASOT(CHRISTCHURCH) LIMITED",  ""  , "1 GARTH EDGE"            , "SHAWFORTH"     , "WHITWORTH"  , "ROCHDALE LANCASHIRE"    ,  ""  , "OL12 8EH", "2", "45220", "40" , "0" , "7" , "7"  , "0", "0", "0","0.509298879","5819"),
    ("1100000005", "9906000175", "HIBAER"                       ,  ""  , "1 GEORGE SQUARE"         , "GLASGOW"       ,      ""      ,       ""                 ,  ""  , "G2 5LL"  , "1", "45200", "11" , "1" , "106", "106", "0", "0", "0", "0.147768898","5819"),
    ("1100000006", "9906000205", "HIBAER"                       ,  ""  , "1 GLEN ROAD"             , "HINDHEAD"      , "SURREY"     ,       ""                 ,  ""  , "GU26 6QE", "1", "45112", "" , "1" , "297", "297", "0", "0", "0", "0.588701588","-2"),
    ("1100000007", "9906000275", "IBANOCTRACTS UK LTD"          ,  ""  , "1 GLYNDE PLACE"          , "HORSHAM"       , "WEST SUSSEX",       ""                 ,  ""  , "RH12 1NZ", "1", "45120", "29" , "2" , "287", "287", "0", "0", "0", "0.155647458","5819"),
    ("1100000008", "9906000325", "TLUBARE"                      ,  ""  , "1 GORSE ROAD"            , "REYDON"        , "SOUTHWOLD"  ,       ""                 ,  ""  , "IP18 6NQ", "1", "45138", "" , "3" , "197", "197", "0", "0", "0", "0.446872271","-2"),
    ("1100000009", "9906000355", "BUCARR"                       ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "45240", "41" , "1" , "18" , "18" , "0", "0", "0", "0.847311602","5819"),
    ("1100000010", "9906000405", "DCAJ&WALTON"                  ,  ""  , "1 GRANVILLE AVENUE"      , "LONG EATON"    , "NOTTINGHAM" ,       ""                 ,  ""  , "NG10 4HA", "1", "45155", "" , "2" , "72" , "72" , "0", "0", "0", "0.548604086","-2"),
    ("1100000011", "9906000415", "&BAMCFLINT"                   ,  ""  , "1 GARENDON WAY"          , "GROBY"         , "LEICESTER"  ,       ""                 ,  ""  , "LE6 0YR" , "1", "45167", "33" , "0" , "400", "400", "0", "0", "0", "0.269071541","5819")
  )

  def getDataDF(implicit spark:SparkSession) = {
    val dataDF = spark.createDataFrame(data).toDF(inputDataColNames: _*)
    dataDF
  }

  def getPropsDF(implicit spark:SparkSession) = {
    val propsDF = spark.createDataFrame(props).toDF(propsColNames: _*)
    propsDF
  }

  def expectedOutputDF(implicit spark:SparkSession) = {
    val expectedDF = spark.createDataFrame(expectedOutputData).toDF(ouputDataColNames: _*)
    expectedDF
  }

}

