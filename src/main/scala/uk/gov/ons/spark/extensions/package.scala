package uk.gov.ons.spark

import org.apache.spark.sql.{DataFrame, Row}


package object sql {


  implicit class SqlRowExtensions(val row:Row){



    def getOption[T](field:String):Option[T]= if(row.isNull(field)) None
    else Option[T](row.getAs[T](field))

    def isNull(field:String):Boolean = try {
      row.isNullAt(row.fieldIndex(field))  //if field exists and the value is null
    }catch {
      case iae:IllegalArgumentException => true  //if field does not exist
      case e: Throwable => {
        println(s"field: ${if (field==null) "null" else field.toString()}")
        throw e
      }
    }
  }

}
