package scala.com.edu.scu.log

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * MySQL tool class
  */
object MySQLUtils {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/log_analysis?user=root&password=root&useSSL=false&serverTimezone=GMT")
  }

  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
