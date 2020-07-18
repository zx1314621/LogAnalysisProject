package scala.com.edu.scu.log

import com.edu.scu.log.{DayCityVideoAccessStat, DayVideoAccessStat, DayVideoTrafficsStat, StatDAO}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * Top N statistic SPARK job
  */
object TopNStatJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob").config("spark.sql.sources.partitionColumnTypeInference.enabled", "false").master("local[2]").getOrCreate()

    val accessDF = spark.read.format("parquet").load("file:///Users/dylan/Desktop/LogAnalysisProject/clean")

    //    accessDF.printSchema()
    //    accessDF.show(10, false)


    val day = "20170511"

    StatDAO.deleteData(day)

    videoAccessTopNStat(spark, accessDF, day)

    articleAccessTopNStat(spark, accessDF, day)

    //top N by city
    cityAccessTopNStat(spark, accessDF, day)

    // data Top N
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()
  }

  // top N courses
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {


    //dataFrame
    // import spark.implicits._
    //    val videoAccessTopN = accessDF.filter($"day" === "20170511" && $"cmsType" === "video")
    //      .groupBy("day","cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
    //
    //

    // sql
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessTopN = spark.sql("select day, cmsId, count(1) as times from access_logs where day = '20170511' and cmsType='video'" +
      "group by day,cmsId order by times desc")


    videoAccessTopN.show(false)


    // write to MySQL
    try {
      videoAccessTopN.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }


  }


  def articleAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._
    val articleAccessTopN = accessDF.filter($"day" === day && $"cmsType" === "article")
      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)

    articleAccessTopN.show(false)

    try {
      articleAccessTopN.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        StatDAO.insertDayArticleAccessTopN(list)
      })


    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._
    val cityAccessTopDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "city", "cmsId").agg(count("cmsId").as("times"))

    //cityAccessTopDF.show()

    val top3DF = cityAccessTopDF.select(
      cityAccessTopDF("day"),
      cityAccessTopDF("city"),
      cityAccessTopDF("cmsId"),
      cityAccessTopDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopDF("city"))
        .orderBy($"times".desc)
      ).as("times_rank")
    ).filter("times_rank <= 3") //.show(false)

    top3DF.show()

    try {
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityVideoAccessStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")

          list.append(DayCityVideoAccessStat(day, cmsId, city, times, times_rank))
        })

        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }




    //cityAccessTopDF.printSchema()
    //cityAccessTopDF.show()
  }

  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._
    val cityAccessTopDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)


    try {
      cityAccessTopDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day, cmsId, traffics))
        })

        StatDAO.insertDayCityVideoTrafficTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }

    cityAccessTopDF.printSchema()
    cityAccessTopDF.show()
  }

}
