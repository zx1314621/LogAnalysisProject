package com.edu.scu.log

import com.ggstar.util.ip.IpHelper

/**
  * IP to City tool class
  */


object IpUtils {

  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getCity("73.223.88.222"))
  }
}
