package com.edu.scu.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer
import scala.com.edu.scu.log.MySQLUtils

object StatDAO {
  /**
    * save to database
    *
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      //close auto commit for batch processing
      connection.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values (?, ?, ?)"

      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }

      // batch processing
      pstmt.executeBatch()

      // commit by person
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def insertDayArticleAccessTopN(list: ListBuffer[DayVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_article_access_topn_stat(day, cms_id, times) values (?, ?, ?)"

      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()

      connection.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def insertDayCityVideoAccessTopN(list: ListBuffer[DayCityVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_video_city_access_topn_stat(day, cms_id, city, times, times_rank) values (?, ?, ?, ?, ?)"

      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)

        pstmt.addBatch()
      }

      pstmt.executeBatch()

      connection.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  def insertDayCityVideoTrafficTopN(list: ListBuffer[DayVideoTrafficsStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into day_video_traffics_topn_stat(day, cms_id, traffics) values (?, ?, ?)"

      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)

        pstmt.addBatch()
      }

      pstmt.executeBatch()

      connection.commit()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
    *
    * delete specific day
    *
    * @param day
    */
  /*
  * day_article_access_topn_stat    |
| day_video_access_topn_stat      |
| day_video_city_access_topn_stat |
| day_video_traffics_topn_stat */
  def deleteData(day: String): Unit = {
    val tables = Array("day_article_access_topn_stat", "day_video_access_topn_stat", "day_video_city_access_topn_stat", "day_video_traffics_topn_stat")

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()

      for (table <- tables) {
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)

        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }

    } catch  {
      case e :Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }
}
