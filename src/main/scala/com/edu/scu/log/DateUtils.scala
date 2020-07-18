package com.edu.scu.log

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/*
* date analysis
* */
object DateUtils {

  // input_format
  //[10/Nov/2016:00:01:02 +0800]

  // SimpleDateFormat can not guarantee thread-safe, it will seperate false date information like 1970-01-01 08:00:00
  //val YYYYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //target_format
//  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  /**
   * get time yyyy-MM-dd HH:mm:ss
   */
  def parse(time:String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }


  /*
  * get Long type time from input
  * */
  def getTime(time:String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1,
        time.lastIndexOf("]"))).getTime
    }catch  {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:02 -0800]"))
  }
}
