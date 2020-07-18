package com.edu.scu.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/*
* spark data clean
* */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file:///Users/dylan/Desktop/LogAnalysisProject/access.log")

    //accessRDD.take(10).foreach(println)


    //RDD => DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)

//    accessDF.printSchema()
//    accessDF.show(10)

    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("day").save("file:///Users/dylan/Desktop/LogAnalysisProject/clean")
    spark.stop()
  }

}
